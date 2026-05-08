package server

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/Altinity/clickhouse-backup/v2/pkg/backup"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
)

// actionsCASHandler handles cas-* verbs sent through POST /backup/actions.
//
// The `command` argument is the first token from the shell-split command line
// (e.g. "cas-upload"). `args` is the full token slice (args[0] == command).
// `row` carries the original raw command string for the status log.
//
// The method mirrors the async pattern of actionsAsyncCommandsHandler: it
// starts a status entry, kicks a goroutine, and immediately appends an
// "acknowledged" result row. For cas-delete (sync-by-convention), it still
// runs asynchronously here so that the /backup/actions endpoint never blocks
// — callers can poll /backup/actions to check completion.
func (api *APIServer) actionsCASHandler(command string, args []string, row status.ActionRow, actionsResults []actionsResultsRow) ([]actionsResultsRow, error) {
	if !api.GetConfig().API.AllowParallel && status.Current.InProgress() {
		return actionsResults, ErrAPILocked
	}
	// Try to reload config from disk; fall back to the cached config when the
	// file is not available (e.g. in unit tests with a stub config path).
	cfg := api.GetConfig()
	if reloaded, reloadErr := api.ReloadConfig(nil, command); reloadErr == nil {
		cfg = reloaded
	}

	operationId, _ := uuid.NewUUID()
	commandId, _ := status.Current.StartWithOperationId(row.Command, operationId.String())
	// No callback URL in the /backup/actions protocol — use the no-op callback.
	noopCb, _ := parseCallback(url.Values{})

	switch command {
	case "cas-upload":
		name, skipObjectDisks, dryRun, waitForPrune, parseErr := parseCASUploadArgs(args[1:], cfg.CAS.WaitForPruneDuration())
		if parseErr != nil {
			status.Current.Stop(commandId, parseErr)
			return actionsResults, parseErr
		}
		go func() {
			err, _ := api.metrics.ExecuteWithMetrics("cas-upload", 0, func() error {
				b := backup.NewBackuper(cfg)
				return b.CASUpload(name, skipObjectDisks, dryRun, api.clickhouseBackupVersion, commandId, waitForPrune)
			})
			status.Current.Stop(commandId, err)
			if err != nil {
				log.Error().Msgf("actions cas-upload error: %v", err)
				api.errorCallback(context.Background(), err, operationId.String(), noopCb)
			} else {
				api.successCallback(context.Background(), operationId.String(), noopCb)
			}
		}()

	case "cas-download":
		name, tablePattern, partitions, schemaOnly, parseErr := parseCASDownloadArgs(args[1:])
		if parseErr != nil {
			status.Current.Stop(commandId, parseErr)
			return actionsResults, parseErr
		}
		go func() {
			err, _ := api.metrics.ExecuteWithMetrics("cas-download", 0, func() error {
				b := backup.NewBackuper(cfg)
				return b.CASDownload(name, tablePattern, partitions, schemaOnly, false, api.clickhouseBackupVersion, commandId)
			})
			status.Current.Stop(commandId, err)
			if err != nil {
				log.Error().Msgf("actions cas-download error: %v", err)
				api.errorCallback(context.Background(), err, operationId.String(), noopCb)
			} else {
				api.successCallback(context.Background(), operationId.String(), noopCb)
			}
		}()

	case "cas-restore":
		name, opts, parseErr := parseCASRestoreArgs(args[1:])
		if parseErr != nil {
			status.Current.Stop(commandId, parseErr)
			return actionsResults, parseErr
		}
		go func() {
			err, _ := api.metrics.ExecuteWithMetrics("cas-restore", 0, func() error {
				b := backup.NewBackuper(cfg)
				return b.CASRestore(
					name, opts.tablePattern,
					opts.dbMapping, opts.tableMapping, opts.partitions, opts.skipProjections,
					opts.schemaOnly, false, // dataOnly always false for CAS
					opts.dropExists, false, // ignoreDependencies always false
					opts.restoreSchemaAsAttach, opts.replicatedCopyToDetached,
					opts.skipEmptyTables, opts.resume,
					api.clickhouseBackupVersion, commandId,
				)
			})
			status.Current.Stop(commandId, err)
			if err != nil {
				log.Error().Msgf("actions cas-restore error: %v", err)
				api.errorCallback(context.Background(), err, operationId.String(), noopCb)
			} else {
				api.successCallback(context.Background(), operationId.String(), noopCb)
			}
		}()

	case "cas-delete":
		name, waitForPrune, parseErr := parseCASDeleteArgs(args[1:], cfg.CAS.WaitForPruneDuration())
		if parseErr != nil {
			status.Current.Stop(commandId, parseErr)
			return actionsResults, parseErr
		}
		go func() {
			err, _ := api.metrics.ExecuteWithMetrics("cas-delete", 0, func() error {
				b := backup.NewBackuper(cfg)
				return b.CASDelete(name, commandId, waitForPrune)
			})
			status.Current.Stop(commandId, err)
			if err != nil {
				log.Error().Msgf("actions cas-delete error: %v", err)
				api.errorCallback(context.Background(), err, operationId.String(), noopCb)
			} else {
				api.successCallback(context.Background(), operationId.String(), noopCb)
			}
		}()

	case "cas-verify":
		if len(args) < 2 || args[1] == "" {
			err := fmt.Errorf("cas-verify: name required")
			status.Current.Stop(commandId, err)
			return actionsResults, err
		}
		name := utils.CleanBackupNameRE.ReplaceAllString(args[1], "")
		go func() {
			err, _ := api.metrics.ExecuteWithMetrics("cas-verify", 0, func() error {
				b := backup.NewBackuper(cfg)
				return b.CASVerify(name, true, commandId)
			})
			status.Current.Stop(commandId, err)
			if err != nil {
				log.Error().Msgf("actions cas-verify error: %v", err)
				api.errorCallback(context.Background(), err, operationId.String(), noopCb)
			} else {
				api.successCallback(context.Background(), operationId.String(), noopCb)
			}
		}()

	case "cas-prune":
		dryRun, graceBlob, abandonThreshold, unlock := parseCASPruneArgs(args[1:])
		if unlock {
			log.Warn().Msg("cas-prune --unlock invoked via /backup/actions; operator override of stranded marker")
		}
		go func() {
			err, _ := api.metrics.ExecuteWithMetrics("cas-prune", 0, func() error {
				b := backup.NewBackuper(cfg)
				return b.CASPrune(dryRun, graceBlob, abandonThreshold, unlock, commandId)
			})
			status.Current.Stop(commandId, err)
			if err != nil {
				log.Error().Msgf("actions cas-prune error: %v", err)
				api.errorCallback(context.Background(), err, operationId.String(), noopCb)
			} else {
				api.successCallback(context.Background(), operationId.String(), noopCb)
			}
		}()

	case "cas-status":
		// cas-status is informational; run async so /backup/actions never blocks.
		go func() {
			b := backup.NewBackuper(cfg)
			_, reportErr := b.CASStatusJSON(commandId)
			status.Current.Stop(commandId, reportErr)
			if reportErr != nil {
				log.Error().Msgf("actions cas-status error: %v", reportErr)
			}
		}()

	default:
		err := fmt.Errorf("actionsCASHandler: unrecognised CAS command %q", command)
		status.Current.Stop(commandId, err)
		return actionsResults, err
	}

	actionsResults = append(actionsResults, actionsResultsRow{
		Status:    "acknowledged",
		Operation: row.Command,
	})
	return actionsResults, nil
}

// ──────────────────────────────────────────────────────────────────
// Argument parsers — consume the token slice that follows the verb.
// ──────────────────────────────────────────────────────────────────

func parseCASUploadArgs(args []string, defaultWaitForPrune time.Duration) (name string, skipObjectDisks, dryRun bool, waitForPrune time.Duration, err error) {
	waitForPrune = defaultWaitForPrune
	for _, a := range args {
		switch {
		case a == "--skip-object-disks":
			skipObjectDisks = true
		case a == "--dry-run":
			dryRun = true
		case strings.HasPrefix(a, "--wait-for-prune="):
			dur, parseErr := time.ParseDuration(strings.TrimPrefix(a, "--wait-for-prune="))
			if parseErr != nil {
				err = fmt.Errorf("cas-upload: %w", parseErr)
				return
			}
			waitForPrune = dur
		case !strings.HasPrefix(a, "-"):
			if name == "" {
				name = utils.CleanBackupNameRE.ReplaceAllString(a, "")
			}
		}
	}
	if name == "" {
		err = fmt.Errorf("cas-upload: name required")
	}
	return
}

func parseCASDownloadArgs(args []string) (name, tablePattern string, partitions []string, schemaOnly bool, err error) {
	for _, a := range args {
		switch {
		case a == "--schema":
			schemaOnly = true
		case strings.HasPrefix(a, "--table="):
			tablePattern = strings.TrimPrefix(a, "--table=")
		case strings.HasPrefix(a, "--partitions="):
			partitions = append(partitions, strings.TrimPrefix(a, "--partitions="))
		case !strings.HasPrefix(a, "-"):
			if name == "" {
				name = utils.CleanBackupNameRE.ReplaceAllString(a, "")
			}
		}
	}
	if name == "" {
		err = fmt.Errorf("cas-download: name required")
	}
	return
}

type casRestoreOpts struct {
	tablePattern             string
	dbMapping                []string
	tableMapping             []string
	partitions               []string
	skipProjections          []string
	schemaOnly               bool
	dropExists               bool
	restoreSchemaAsAttach    bool
	replicatedCopyToDetached bool
	skipEmptyTables          bool
	resume                   bool
}

func parseCASRestoreArgs(args []string) (name string, opts casRestoreOpts, err error) {
	for _, a := range args {
		switch {
		case a == "--schema":
			opts.schemaOnly = true
		case a == "--drop" || a == "--rm":
			opts.dropExists = true
		case a == "--restore-schema-as-attach":
			opts.restoreSchemaAsAttach = true
		case a == "--replicated-copy-to-detached":
			opts.replicatedCopyToDetached = true
		case a == "--skip-empty-tables":
			opts.skipEmptyTables = true
		case a == "--resume" || a == "--resumable":
			opts.resume = true
		case strings.HasPrefix(a, "--table="):
			opts.tablePattern = strings.TrimPrefix(a, "--table=")
		case strings.HasPrefix(a, "--partitions="):
			opts.partitions = append(opts.partitions, strings.TrimPrefix(a, "--partitions="))
		case strings.HasPrefix(a, "--skip-projections="):
			opts.skipProjections = append(opts.skipProjections, strings.TrimPrefix(a, "--skip-projections="))
		case strings.HasPrefix(a, "--restore-database-mapping="):
			for _, m := range strings.Split(strings.TrimPrefix(a, "--restore-database-mapping="), ",") {
				if m = strings.TrimSpace(m); m != "" {
					opts.dbMapping = append(opts.dbMapping, m)
				}
			}
		case strings.HasPrefix(a, "--restore-table-mapping="):
			for _, m := range strings.Split(strings.TrimPrefix(a, "--restore-table-mapping="), ",") {
				if m = strings.TrimSpace(m); m != "" {
					opts.tableMapping = append(opts.tableMapping, m)
				}
			}
		case !strings.HasPrefix(a, "-"):
			if name == "" {
				name = utils.CleanBackupNameRE.ReplaceAllString(a, "")
			}
		}
	}
	if name == "" {
		err = fmt.Errorf("cas-restore: name required")
	}
	return
}

func parseCASDeleteArgs(args []string, defaultWaitForPrune time.Duration) (name string, waitForPrune time.Duration, err error) {
	waitForPrune = defaultWaitForPrune
	for _, a := range args {
		switch {
		case strings.HasPrefix(a, "--wait-for-prune="):
			dur, parseErr := time.ParseDuration(strings.TrimPrefix(a, "--wait-for-prune="))
			if parseErr != nil {
				err = fmt.Errorf("cas-delete: %w", parseErr)
				return
			}
			waitForPrune = dur
		case !strings.HasPrefix(a, "-"):
			if name == "" {
				name = utils.CleanBackupNameRE.ReplaceAllString(a, "")
			}
		}
	}
	if name == "" {
		err = fmt.Errorf("cas-delete: name required")
	}
	return
}

func parseCASPruneArgs(args []string) (dryRun bool, graceBlob, abandonThreshold string, unlock bool) {
	for _, a := range args {
		switch {
		case a == "--dry-run":
			dryRun = true
		case a == "--unlock":
			unlock = true
		case strings.HasPrefix(a, "--grace-blob="):
			graceBlob = strings.TrimPrefix(a, "--grace-blob=")
		case strings.HasPrefix(a, "--abandon-threshold="):
			abandonThreshold = strings.TrimPrefix(a, "--abandon-threshold=")
		}
	}
	return
}
