package clickhouse

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/apex/log"
)

// ComputePartitionsDelta - computes the data partitions to be added and removed between live and backup tables
func (ch *ClickHouse) ComputePartitionsDelta(restoreTables []metadata.TableMetadata, liveTables []Table) ([]PartDiff, error) {
	var ftables []PartDiff
	log.Debugf("Compute partitions discrepancies")
	for _, rtable := range restoreTables {
		var partsToAdd []metadata.Part
		var partsToRemove []metadata.Part
		for _, rpartition := range rtable.Parts["default"] {
			bfound := false
			for _, liveTable := range liveTables {
				if liveTable.Database == rtable.Database && liveTable.Name == rtable.Table {
					livePartitions, _ := ch.GetPartitions(liveTable)
					for _, livePartition := range livePartitions["default"] {
						if livePartition.HashOfAllFiles == rpartition.HashOfAllFiles && livePartition.HashOfUncompressedFiles == rpartition.HashOfUncompressedFiles && livePartition.UncompressedHashOfCompressedFiles == rpartition.UncompressedHashOfCompressedFiles {
							bfound = true
							break
						}
					}
				}
			}
			if !bfound {
				partsToAdd = append(partsToAdd, metadata.Part{Name: rpartition.Name, Path: rpartition.Path})
			}
		}

		for _, ltable := range liveTables {
			if ltable.Name == rtable.Table {
				partitions, _ := ch.GetPartitions(ltable)
				for _, livepart := range partitions["default"] {
					bfound := false
					for _, backuppart := range rtable.Parts["default"] {
						if livepart.HashOfAllFiles == backuppart.HashOfAllFiles && livepart.HashOfUncompressedFiles == backuppart.HashOfUncompressedFiles && livepart.UncompressedHashOfCompressedFiles == backuppart.UncompressedHashOfCompressedFiles {
							bfound = true
							break
						}
					}
					if !bfound {
						partsToRemove = append(partsToRemove, livepart)
					}
				}
			}
		}
		log.Debugf("[%s.%s] Backup data parts to attach : %v ", rtable.Database, rtable.Table, partsToAdd)
		log.Debugf("[%s.%s] Live data parts to detach : %v ", rtable.Database, rtable.Table, partsToRemove)
		ftables = append(ftables, PartDiff{rtable, partsToAdd, partsToRemove})
	}
	log.Debugf("Compute partitions discrepancies. Done")

	return ftables, nil
}

// CopyDataDiff - copy only partitions that will be attached to "detached" folder
func (ch *ClickHouse) CopyDataDiff(diff PartDiff) error {
	log.Debugf("Prepare data for restoring '%s.%s'", diff.BTable.Database, diff.BTable.Table)
	dataPath, err := ch.GetDefaultPath()
	if err != nil {
		return err
	}
	detachedParentDir := filepath.Join(dataPath, "data", TablePathEncode(diff.BTable.Database), TablePathEncode(diff.BTable.Table), "detached")
	os.MkdirAll(detachedParentDir, 0750)
	ch.Chown(detachedParentDir)

	for _, partition := range diff.PartitionsAdd {
		log.Debugf("Processing partition %s (%s)", partition.Name, partition.Path)
		detachedPath := filepath.Join(detachedParentDir, partition.Name)
		info, err := os.Stat(detachedPath)
		if err != nil {
			if os.IsNotExist(err) {
				// partition dir does not exist, creating
				os.MkdirAll(detachedPath, 0750)
			} else {
				return err
			}
		} else if !info.IsDir() {
			return fmt.Errorf("'%s' should be directory or absent", detachedPath)
		}
		ch.Chown(detachedPath)

		if err := filepath.Walk(partition.Path, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			filePath = filepath.ToSlash(filePath) // fix Windows slashes
			filename := strings.Trim(strings.TrimPrefix(filePath, partition.Path), "/")
			dstFilePath := filepath.Join(detachedPath, filename)
			if info.IsDir() {
				os.MkdirAll(dstFilePath, 0750)
				return ch.Chown(dstFilePath)
			}
			if !info.Mode().IsRegular() {
				log.Debugf("'%s' is not a regular file, skipping.", filePath)
				return nil
			}
			if err := os.Link(filePath, dstFilePath); err != nil {
				return fmt.Errorf("failed to crete hard link '%s' -> '%s': %v", filePath, dstFilePath, err)
			}
			return ch.Chown(dstFilePath)
		}); err != nil {
			return fmt.Errorf("error during filepath.Walk for partition '%s': %v", partition.Path, err)
		}
	}
	log.Debugf("Prepare data for restoring '%s.%s'. DONE", diff.BTable.Database, diff.BTable.Table)
	return nil
}

// ApplyPartitionsChanges - add/remove partitions to/from the live table
func (ch *ClickHouse) ApplyPartitionsChanges(table PartDiff) error {
	var query string
	dataPath, err := ch.GetDefaultPath()
	if err != nil {
		return err
	}
	for _, partition := range table.PartitionsAdd {
		query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PART '%s'", table.BTable.Database, table.BTable.Table, partition.Name)
		log.Debug(query)
		if _, err := ch.conn.Exec(query); err != nil {
			return err
		}
	}

	if len(table.PartitionsRemove) > 0 {
		partList := make(map[string]struct{})

		for _, partition := range table.PartitionsRemove {
			log.Debugf("Removing %s", partition.Path)

			//partitionName := partition.Name[:strings.IndexByte(partition.Name, '_')]
			partitionName := partition.Partition

			if _, ok := partList[partitionName]; !ok {
				partList[partitionName] = struct{}{}
			}
		}

		for partname := range partList {
			/*if partname == "all" {
				query = fmt.Sprintf("DETACH TABLE `%s`.`%s`", table.btable.Database, table.btable.Name)
			} else {*/
			query = fmt.Sprintf("ALTER TABLE `%s`.`%s` DETACH PARTITION %s", table.BTable.Database, table.BTable.Table, partname)
			//}
			log.Debugf(query)
			if _, err := ch.conn.Exec(query); err != nil {
				return err
			}
		}

		detachedParentDir := filepath.Join(dataPath, "data", TablePathEncode(table.BTable.Database), TablePathEncode(table.BTable.Table), "detached")

		for _, partition := range table.PartitionsRemove {
			detachedPath := filepath.Join(detachedParentDir, partition.Name)
			log.Debugf("[%s.%s] Removing %s", table.BTable.Database, table.BTable.Table, detachedPath)
			e := os.RemoveAll(detachedPath)
			if e != nil {
				return e
			}
		}

		for partname := range partList {
			if partname == "all" {
				query = fmt.Sprintf("ATTACH TABLE `%s`.`%s`", table.BTable.Database, table.BTable.Table)
			} else {
				query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION %s", table.BTable.Database, table.BTable.Table, partname)
			}
			log.Debugf(query)
			if _, err := ch.conn.Exec(query); err != nil {
				return err
			}
		}
		/*e := os.RemoveAll(partition.Path)
		if e != nil {
			return e
		}*/

		/*query = fmt.Sprintf("ATTACH TABLE `%s`.`%s`", table.btable.Database, table.btable.Name)
		log.Println(query)
		if _, err := ch.conn.Exec(query); err != nil {
			return err
		}*/
	}
	return nil
}
