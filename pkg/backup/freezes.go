package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	bolt "go.etcd.io/bbolt"
)

// freezesFileName - bbolt file inside <backup_name>/ which records the `FREEZE ... WITH NAME <uuid>` names
// issued by `create` and not yet unfrozen; it survives SIGKILL/OOM so `clean`, `delete local` and
// `clean_local_broken` can unfreeze the orphaned shadow/<uuid> directories later,
// see https://github.com/Altinity/clickhouse-backup/issues/1563
const freezesFileName = "freezes.tmp"

var freezesBucket = []byte("freezes")

type freezeRecord struct {
	Database  string    `json:"database"`
	Table     string    `json:"table"`
	CreatedAt time.Time `json:"created_at"`
}

type freezesState struct {
	file string
	db   *bolt.DB
}

func freezesFilePath(defaultDataPath, backupName string) string {
	return path.Join(defaultDataPath, "backup", backupName, freezesFileName)
}

// openFreezes opens or creates the freezes file, bbolt holds an exclusive flock on it while open,
// so the 1s timeout also detects a parallel clickhouse-backup process which still works on the same backup
func openFreezes(file string) (*freezesState, error) {
	if err := os.MkdirAll(path.Dir(file), 0755); err != nil {
		return nil, errors.Wrapf(err, "os.MkdirAll %s", path.Dir(file))
	}
	db, err := bolt.Open(file, 0644, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, errors.Wrapf(err, "bolt.Open %s", file)
	}
	if err = db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucketIfNotExists(freezesBucket)
		return createErr
	}); err != nil {
		_ = db.Close()
		return nil, errors.Wrapf(err, "bolt.CreateBucketIfNotExists %s", file)
	}
	return &freezesState{file: file, db: db}, nil
}

func (f *freezesState) add(uuid string, rec freezeRecord) error {
	value, err := json.Marshal(rec)
	if err != nil {
		return errors.Wrap(err, "json.Marshal freezeRecord")
	}
	return f.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(freezesBucket).Put([]byte(uuid), value)
	})
}

func (f *freezesState) remove(uuid string) error {
	return f.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(freezesBucket).Delete([]byte(uuid))
	})
}

func (f *freezesState) list() (map[string]freezeRecord, error) {
	result := map[string]freezeRecord{}
	err := f.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(freezesBucket).ForEach(func(k, v []byte) error {
			var rec freezeRecord
			if unmarshalErr := json.Unmarshal(v, &rec); unmarshalErr != nil {
				log.Warn().Msgf("freezes: can't parse record %s in %s: %v", string(k), f.file, unmarshalErr)
			}
			result[string(k)] = rec
			return nil
		})
	})
	return result, err
}

func (f *freezesState) close() error {
	return f.db.Close()
}

// closeAndRemove deletes the freezes file when no records are left, the file is kept otherwise
func (f *freezesState) closeAndRemove() error {
	remaining, err := f.list()
	if closeErr := f.close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if len(remaining) > 0 {
		return nil
	}
	if err = os.Remove(f.file); err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "os.Remove %s", f.file)
	}
	return nil
}

// addFreeze records uuid before `FREEZE ... WITH NAME uuid` is executed for table
func (b *Backuper) addFreeze(backupName, uuid string, table *clickhouse.Table) error {
	b.freezesMutex.Lock()
	defer b.freezesMutex.Unlock()
	if b.freezes == nil {
		f, err := openFreezes(freezesFilePath(b.DefaultDataPath, backupName))
		if err != nil {
			return err
		}
		b.freezes = f
	}
	return b.freezes.add(uuid, freezeRecord{Database: table.Database, Table: table.Name, CreatedAt: time.Now()})
}

// removeFreeze forgets uuid after the table was unfrozen, the record is kept when shadow/<uuid> still
// exists on any disk so the final cleanOwnFreezes pass removes it
func (b *Backuper) removeFreeze(uuid string, disks []clickhouse.Disk) error {
	for _, disk := range disks {
		if disk.IsBackup {
			continue
		}
		if _, err := os.Stat(path.Join(disk.Path, "shadow", uuid)); err == nil {
			return nil
		}
	}
	b.freezesMutex.Lock()
	defer b.freezesMutex.Unlock()
	if b.freezes == nil {
		return nil
	}
	return b.freezes.remove(uuid)
}

// cleanOwnFreezes unfreezes every shadow uuid still recorded by the current backup and deletes the
// freezes file, it runs on both the success and the failure path of `create`, a canceled command context
// must not stop it so it uses its own context, only clean shadow UUIDs created by this backup,
// don't touch other shadows, see https://github.com/Altinity/clickhouse-backup/issues/1345
func (b *Backuper) cleanOwnFreezes(disks []clickhouse.Disk) error {
	b.freezesMutex.Lock()
	f := b.freezes
	b.freezes = nil
	b.freezesMutex.Unlock()
	if f == nil {
		return nil
	}
	records, err := f.list()
	if err != nil {
		_ = f.close()
		return errors.Wrapf(err, "freezes.list %s", f.file)
	}
	ctx := context.Background()
	for uuid, rec := range records {
		if err = b.unfreezeShadow(ctx, uuid, rec, disks); err != nil {
			_ = f.close()
			return err
		}
		if err = f.remove(uuid); err != nil {
			_ = f.close()
			return errors.Wrapf(err, "freezes.remove %s", uuid)
		}
	}
	return f.closeAndRemove()
}

// cleanBackupFreezes unfreezes the shadow uuids recorded in <backup_name>/freezes.tmp of a backup which is not
// processed by a live clickhouse-backup process (pid file or bbolt lock), returns inUse=true when it was skipped
func (b *Backuper) cleanBackupFreezes(ctx context.Context, backupName string, disks []clickhouse.Disk) (bool, error) {
	if b.DefaultDataPath == "" {
		defaultDataPath, err := b.ch.GetDefaultPath(disks)
		if err != nil {
			return false, ErrUnknownClickhouseDataPath
		}
		b.DefaultDataPath = defaultDataPath
	}
	file := freezesFilePath(b.DefaultDataPath, backupName)
	if _, err := os.Stat(file); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "os.Stat %s", file)
	}
	if pidlock.IsRunning(backupName) {
		log.Warn().Str("backup", backupName).Msgf("skip %s, another clickhouse-backup process still works on this backup", file)
		return true, nil
	}
	f, err := openFreezes(file)
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			log.Warn().Str("backup", backupName).Msgf("skip %s, locked by another clickhouse-backup process", file)
			return true, nil
		}
		return false, err
	}
	records, err := f.list()
	if err != nil {
		_ = f.close()
		return false, errors.Wrapf(err, "freezes.list %s", file)
	}
	for uuid, rec := range records {
		if err = b.unfreezeShadow(ctx, uuid, rec, disks); err != nil {
			_ = f.close()
			return false, err
		}
		if b.DryRun {
			continue
		}
		if err = f.remove(uuid); err != nil {
			_ = f.close()
			return false, errors.Wrapf(err, "freezes.remove %s", uuid)
		}
	}
	if b.DryRun {
		return false, f.close()
	}
	return false, f.closeAndRemove()
}

// unfreezeShadow removes shadow/<uuid> from every disk
func (b *Backuper) unfreezeShadow(ctx context.Context, uuid string, rec freezeRecord, disks []clickhouse.Disk) error {
	logger := log.With().Str("shadow", uuid).Str("database", rec.Database).Str("table", rec.Table).Logger()
	if b.DryRun {
		logger.Info().Msg("dry-run: would unfreeze shadow")
		return nil
	}
	version, err := b.ch.GetVersion(ctx)
	if err != nil {
		return errors.Wrap(err, "b.ch.GetVersion")
	}
	// a canceled command aborts `ALTER TABLE ... FREEZE ... WITH NAME <uuid>` on the client side only,
	// the server keeps executing it and recreates shadow/<uuid> right after we removed it,
	// so wait for those queries to leave system.processes first
	b.waitFreezeQueriesDone(ctx, uuid)
	unfreezeQuery := ""
	if version > 21004000 && rec.Table != "" {
		unfreezeQuery = fmt.Sprintf("ALTER TABLE `%s`.`%s` UNFREEZE WITH NAME '%s'", rec.Database, rec.Table, uuid)
	} else if version >= 22006000 {
		unfreezeQuery = fmt.Sprintf("SYSTEM UNFREEZE WITH NAME '%s'", uuid)
	}
	if unfreezeQuery != "" {
		if err = b.ch.QueryContext(ctx, unfreezeQuery); err != nil {
			logger.Warn().Msgf("%s failed, remove shadow directories directly: %v", unfreezeQuery, err)
		}
	}
	for _, disk := range disks {
		if disk.IsBackup {
			continue
		}
		shadowDir := path.Join(disk.Path, "shadow", uuid)
		removed, removeErr := removeShadowDir(shadowDir)
		if removeErr != nil {
			return removeErr
		}
		if removed {
			logger.Info().Msgf("cleaned shadow %s", shadowDir)
		}
	}
	return nil
}

// removeShadowDir deletes shadowDir and makes sure it stays deleted: an interrupted FREEZE is still
// creating hardlinks in it for a short while after its query left system.processes, so the directory
// can reappear right after the first removal, see https://github.com/Altinity/clickhouse-backup/issues/1563
func removeShadowDir(shadowDir string) (bool, error) {
	const attempts = 10
	const settleInterval = 200 * time.Millisecond
	removed := false
	for attempt := 0; attempt < attempts; attempt++ {
		if _, statErr := os.Stat(shadowDir); statErr != nil {
			if os.IsNotExist(statErr) {
				return removed, nil
			}
			return removed, errors.Wrapf(statErr, "os.Stat %s", shadowDir)
		}
		if err := os.RemoveAll(shadowDir); err != nil {
			return removed, errors.Wrapf(err, "can't clean shadow '%s'", shadowDir)
		}
		removed = true
		// the directory can reappear while the interrupted FREEZE finishes its hardlinks,
		// so check once more after a settle interval instead of trusting a single removal
		time.Sleep(settleInterval)
	}
	return removed, errors.Errorf("shadow '%s' keeps reappearing after %d removals, a FREEZE query is probably still running", shadowDir, attempts)
}

// waitFreezeQueriesDone waits until no FREEZE query of the shadow uuid is running on the server,
// best effort: an unreadable system.processes or the timeout only produce a warning.
// The `ALTER TABLE%FREEZE%` prefix keeps this very query, which carries the uuid in its own text,
// out of the result
func (b *Backuper) waitFreezeQueriesDone(ctx context.Context, uuid string) {
	const waitTimeout = 30 * time.Second
	const pollInterval = 200 * time.Millisecond
	query := fmt.Sprintf("SELECT count() AS cnt FROM system.processes WHERE query LIKE 'ALTER TABLE%%FREEZE%%' AND query LIKE '%%%s%%' SETTINGS empty_result_for_aggregation_by_empty_set=0", uuid)
	deadline := time.Now().Add(waitTimeout)
	for {
		var running uint64
		if err := b.ch.SelectSingleRow(ctx, &running, query); err != nil {
			log.Warn().Msgf("can't check running FREEZE queries for shadow %s: %v", uuid, err)
			return
		}
		if running == 0 {
			return
		}
		if time.Now().After(deadline) {
			log.Warn().Msgf("%d FREEZE queries for shadow %s are still running after %s, remove it anyway", running, uuid, waitTimeout)
			return
		}
		log.Debug().Msgf("waiting for %d running FREEZE queries of shadow %s", running, uuid)
		time.Sleep(pollInterval)
	}
}
