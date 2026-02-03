//go:build integration

package main

import (
	"testing"
	"time"
)

func TestListFormat(t *testing.T) {
	env, r := NewTestEnvironment(t)
	env.connectWithWait(t, r, 0*time.Second, 1*time.Second, 1*time.Minute)

	// Create a test backup to have something to list
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-ce", "ALLOW_EMPTY_BACKUPS=true clickhouse-backup -c /etc/clickhouse-backup/config-s3.yml create test_list_format_backup")
	out, err := env.DockerExecOut("clickhouse-backup", "bash", "-ce", "cat /var/lib/clickhouse/backup/test_list_format_backup/metadata.json")
	r.NoError(err)
	r.Contains(out, "\"tables\": null")

	// Test text format (default)
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list")
	r.NoError(err)
	r.Contains(out, "test_list_format_backup")
	r.Contains(out, "all:0B,data:0B,arch:0B,obj:0B,meta:0B,rbac:0B,conf:0B")

	// Test JSON format
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "--format", "json")
	r.NoError(err)
	r.Contains(out, "\"BackupName\":\"test_list_format_backup\"")
	r.Contains(out, "\"Size\":\"all:0B,data:0B,arch:0B,obj:0B,meta:0B,rbac:0B,conf:0B,nc:0B\"")
	r.Contains(out, "\"Description\":\"regular\",\"RequiredBackup\":\"\",\"Type\":\"local\"")

	// Test YAML format
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "--format", "yaml")
	r.NoError(err)
	r.Contains(out, "- backupname: test_list_format_backup")
	r.Contains(out, "size: all:0B,data:0B,arch:0B,obj:0B,meta:0B,rbac:0B,conf:0B,nc:0B")
	r.Contains(out, "description: regular")
	r.Contains(out, "requiredbackup: \"\"")
	r.Contains(out, "type: local")

	// Test CSV format
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "--format", "csv")
	r.NoError(err)
	r.Contains(out, "test_list_format_backup")

	// Test TSV format
	out, err = env.DockerExecOut("clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "list", "--format", "tsv")
	r.NoError(err)
	r.Contains(out, "test_list_format_backup")

	// Clean up
	env.DockerExecNoError(r, "clickhouse-backup", "clickhouse-backup", "-c", "/etc/clickhouse-backup/config-s3.yml", "delete", "local", "test_list_format_backup")
	env.Cleanup(t, r)
}
