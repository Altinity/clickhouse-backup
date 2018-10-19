package backup_test

import (
	"fmt"
	"log"
	"net/http"
	"testing"

	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
)

const DOCKER_HOST = "testelastic"

func TestMain(m *testing.M) {

	pool, err := dockertest.NewPool(fmt.Sprintf("%s:2375", DOCKER_HOST))
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	minioContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       "minio",
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		PortBindings: map[dc.Port][]dc.PortBinding{
			"9000": []dc.PortBinding{{HostPort: "9000"}},
		},
		Env: []string{"MINIO_ACCESS_KEY=TestKey", "MINIO_SECRET_KEY=TestSecret"},
	})
	if err != nil {
		log.Fatalf("Could not start minio container with %v", err)
	}
	defer pool.Purge(minioContainer)

	clickhouseContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       "clickhouse",
		Repository: "yandex/clickhouse-server",
		Tag:        "18.12.17",
		Cmd:        []string{"server", "/data"},
		PortBindings: map[dc.Port][]dc.PortBinding{
			"8123": []dc.PortBinding{{HostPort: "8123"}},
		},
	})
	if err != nil {
		log.Fatalf("Could not start clickhouse container with %v", err)
	}
	defer pool.Purge(clickhouseContainer)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		_, err := http.Get(fmt.Sprintf("http://%s:%s", DOCKER_HOST, minioContainer.GetPort("9000/tcp")))
		return err
	}); err != nil {
		log.Fatalf("Could not connect to minio with %v", err)
	}
	if err := pool.Retry(func() error {
		_, err := http.Get(fmt.Sprintf("http://%s:%s", DOCKER_HOST, clickhouseContainer.GetPort("8123/tcp")))
		return err
	}); err != nil {
		log.Fatalf("Could not connect to clickhouse with %v", err)
	}
	// Create test data - ch
	// exec freeze and upload - d
	// delete database and shadows - d
	// download data and restore - d
	// check data - ch

	// d := dexec.Docker{c}
	// clickhouseContainerExec, _ := dexec.ByCreatingContainer(
	// 	dc.CreateContainerOptions{Config: &dc.Config{Image: "clickhouse"}})

	// cmd := d.Command(clickhouseContainerExec, "echo", `I am running inside a container!`)
	// b, err := cmd.Output()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Printf("%s", b)

	// c, err := dc.NewClient(fmt.Sprintf("%s:2375", DOCKER_HOST))
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// c.StartExec(clickhouseContainer.Container.ID, dc.StartExecOptions{
	// 	InputStream: o
	// })
}
func TestFreeze(t *testing.T) {

}

func TestSync(t *testing.T) {

}

func TestDownload(t *testing.T) {

}

func TestRestore(t *testing.T) {

}
