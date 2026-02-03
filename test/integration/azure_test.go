//go:build integration

package main

import (
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
)

func TestAzure(t *testing.T) {
	if isTestShouldSkip("AZURE_TESTS") {
		t.Skip("Skipping Azure integration tests...")
		return
	}
	env, r := NewTestEnvironment(t)
	sasCmd := []string{
		"compose", "--project-name", env.ProjectName,
		"-f", path.Join(os.Getenv("CUR_DIR"), os.Getenv("COMPOSE_FILE")),
		"--profile", "azure-cli", "--progress", "none",
		"run", "--rm", "azure-cli",
		"sh", "-c",
		"az storage account generate-sas --account-name=devcontainer1 " +
			"--resource-types=sco --services=b --permissions=cdlruwap --output=tsv " +
			"--expiry " + time.Now().Add(30*time.Hour).Format("2006-01-02T15:04:05Z") +
			" 2>/dev/null",
	}
	sasToken, err := utils.ExecCmdOut(t.Context(), dockerExecTimeout, "docker", sasCmd...)
	sasToken = strings.Trim(sasToken, " \t\r\n")
	r.NoError(err, "unexpected error sasToken=%s", sasToken)
	env.InstallDebIfNotExists(r, "clickhouse-backup", "gettext-base")
	env.DockerExecNoError(r, "clickhouse-backup", "bash", "-xec", "export SAS_TOKEN='"+sasToken+"'; cat /etc/clickhouse-backup/config-azblob-sas.yml.template | envsubst > /etc/clickhouse-backup/config-azblob-sas.yml")
	env.runMainIntegrationScenario(t, "AZBLOB", "config-azblob-sas.yml")
	env.runMainIntegrationScenario(t, "AZBLOB", "config-azblob.yml")
	env.Cleanup(t, r)
}
