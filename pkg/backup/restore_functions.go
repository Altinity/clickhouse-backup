package backup

import (
	"context"
	"strings"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	functionRestoreCreate        = "create"
	functionRestoreReplace       = "replace"
	functionRestoreKeepExisting  = "keep_existing"
	functionRestoreSkipIdentical = "skip_identical"
)

type functionRestoreAction struct {
	Function metadata.FunctionsMeta
	Action   string
}

// planFunctionsRestore decides what to do with each function from backup, existing function with the same definition
// is skipped without any query, different definition is overwritten only with --rm, like tables and RBAC objects,
// https://github.com/Altinity/clickhouse-backup/issues/1636
func planFunctionsRestore(backupFunctions []metadata.FunctionsMeta, existingFunctions []clickhouse.Function, dropExists bool) []functionRestoreAction {
	existing := make(map[string]string, len(existingFunctions))
	for _, f := range existingFunctions {
		existing[f.Name] = normalizeFunctionCreateQuery(f.CreateQuery)
	}
	plan := make([]functionRestoreAction, 0, len(backupFunctions))
	for _, f := range backupFunctions {
		action := functionRestoreCreate
		if existingQuery, exists := existing[f.Name]; exists {
			switch {
			case existingQuery == normalizeFunctionCreateQuery(f.CreateQuery):
				action = functionRestoreSkipIdentical
			case dropExists:
				action = functionRestoreReplace
			default:
				action = functionRestoreKeepExisting
			}
		}
		plan = append(plan, functionRestoreAction{Function: f, Action: action})
	}
	return plan
}

// normalizeFunctionCreateQuery returns `CREATE OR REPLACE FUNCTION ...`, GetUserDefinedFunctions stores functions in backup in this form
func normalizeFunctionCreateQuery(query string) string {
	query = strings.TrimSpace(query)
	if !strings.HasPrefix(query, "CREATE OR REPLACE FUNCTION") {
		query = strings.Replace(query, "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)
	}
	return query
}

// functionsRestorePlan reads existing functions and returns restore plan, used by restore and restore --dry-run
func (b *Backuper) functionsRestorePlan(ctx context.Context, backupFunctions []metadata.FunctionsMeta, dropExists bool) ([]functionRestoreAction, error) {
	if len(backupFunctions) == 0 {
		return nil, nil
	}
	existingFunctions, err := b.ch.GetUserDefinedFunctions(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "GetUserDefinedFunctions")
	}
	return planFunctionsRestore(backupFunctions, existingFunctions, dropExists), nil
}

func (b *Backuper) restoreFunctions(ctx context.Context, backupMetadata metadata.BackupMetadata, dropExists bool) error {
	plan, err := b.functionsRestorePlan(ctx, backupMetadata.Functions, dropExists)
	if err != nil {
		return err
	}
	needCreate := false
	for _, p := range plan {
		if p.Action == functionRestoreCreate || p.Action == functionRestoreReplace {
			needCreate = true
			break
		}
	}
	// https://github.com/Altinity/clickhouse-backup/issues/1123
	onCluster := b.cfg.General.RestoreSchemaOnCluster
	if onCluster != "" && needCreate {
		configFile, doc, configErr := b.ch.ParseXML(ctx, "config.xml")
		if configErr != nil {
			return errors.Wrapf(configErr, "can't parse %s", configFile)
		}
		userDefinedKeeperPathNode := doc.SelectElement("//user_defined_zookeeper_path")
		if userDefinedKeeperPathNode != nil {
			userDefinedKeeperPath := strings.Trim(userDefinedKeeperPathNode.InnerText(), " \t\r\n")
			if userDefinedKeeperPath != "" {
				log.Warn().Msgf("%s contains <user_defined_zookeeper_path>%s</user_defined_zookeeper_path> ON CLUSTER '%s' will ignored during functions restore", configFile, userDefinedKeeperPath, onCluster)
				onCluster = ""
			}
		}
	}

	for _, p := range plan {
		query := normalizeFunctionCreateQuery(p.Function.CreateQuery)
		switch p.Action {
		case functionRestoreSkipIdentical:
			log.Debug().Str("function", p.Function.Name).Msg("function already exists with the same definition, skip")
			continue
		case functionRestoreKeepExisting:
			log.Warn().Str("function", p.Function.Name).Msg("function already exists with different definition, keep existing, use --rm to overwrite it from backup")
			continue
		case functionRestoreCreate:
			// IF NOT EXISTS doesn't overwrite function which exists only on other nodes with ON CLUSTER
			query = strings.Replace(query, "CREATE OR REPLACE FUNCTION", "CREATE FUNCTION IF NOT EXISTS", 1)
		}
		if funcErr := b.ch.CreateUserDefinedFunction(query, onCluster); funcErr != nil {
			return errors.Wrapf(funcErr, "CreateUserDefinedFunction %s", p.Function.Name)
		}
		log.Info().Str("function", p.Function.Name).Str("action", p.Action).Msg("function restored")
	}
	return nil
}
