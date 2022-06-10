package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/pkg/utils"

	"github.com/AlexAkulov/clickhouse-backup/pkg/backup"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"

	apexLog "github.com/apex/log"
	"github.com/google/shlex"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli"
)

const (
	// APITimeFormat - clickhouse compatibility time format
	APITimeFormat  = "2006-01-02 15:04:05"
	InProgressText = "in progress"
)

type APIServer struct {
	c                       *cli.App
	configPath              string
	config                  *config.Config
	server                  *http.Server
	restart                 chan struct{}
	status                  *AsyncStatus
	metrics                 Metrics
	routes                  []string
	clickhouseBackupVersion string
}

type AsyncStatus struct {
	commands []ActionRow
	sync.RWMutex
}

type ActionRow struct {
	Command string `json:"command"`
	Status  string `json:"status"`
	Start   string `json:"start,omitempty"`
	Finish  string `json:"finish,omitempty"`
	Error   string `json:"error,omitempty"`
}

func (status *AsyncStatus) start(command string) int {
	status.Lock()
	defer status.Unlock()
	status.commands = append(status.commands, ActionRow{
		Command: command,
		Start:   time.Now().Format(APITimeFormat),
		Status:  InProgressText,
	})
	lastCommandId := len(status.commands) - 1
	apexLog.Debugf("api.status.Start -> status.commands[%d] == %v", lastCommandId, status.commands[lastCommandId])
	return lastCommandId
}

func (status *AsyncStatus) inProgress() bool {
	status.RLock()
	defer status.RUnlock()
	n := len(status.commands) - 1
	if n < 0 {
		apexLog.Debugf("api.status.inProgress -> len(status.commands)=%d, inProgress=false", len(status.commands))
		return false
	}
	apexLog.Debugf("api.status.inProgress -> status.commands[n].Status == %s, inProgress=%v", status.commands[n].Status, status.commands[n].Status == InProgressText)
	return status.commands[n].Status == InProgressText
}

func (status *AsyncStatus) stop(commandId int, err error) {
	status.Lock()
	defer status.Unlock()
	s := "success"
	if err != nil {
		s = "error"
		status.commands[commandId].Error = err.Error()
	}
	status.commands[commandId].Status = s
	status.commands[commandId].Finish = time.Now().Format(APITimeFormat)
	apexLog.Debugf("api.status.stop -> status.commands[%d] == %v", commandId, status.commands[commandId])
}

func (status *AsyncStatus) status(current bool, filter string, last int) []ActionRow {
	status.RLock()
	defer status.RUnlock()
	if current {
		last = 1
	}
	commands := &status.commands
	l := len(*commands)
	if l == 0 {
		return status.commands
	}

	filteredCommands := make([]ActionRow, 0)
	if filter != "" {
		for _, command := range *commands {
			if strings.Contains(command.Command, filter) || strings.Contains(command.Status, filter) || strings.Contains(command.Error, filter) {
				filteredCommands = append(filteredCommands, command)
			}
		}
		if len(filteredCommands) == 0 {
			return filteredCommands
		}
		commands = &filteredCommands
	}

	begin, end := 0, 1
	l = len(*commands)
	if last > 0 && l > last {
		begin = l - last
		end = l
	} else {
		begin = 0
		end = l
	}
	return (*commands)[begin:end]
}

var (
	ErrAPILocked = errors.New("another operation is currently running")
)

// Server - expose CLI commands as REST API
func Server(c *cli.App, configPath string, clickhouseBackupVersion string) error {
	var (
		cfg *config.Config
		err error
	)
	apexLog.Debug("Wait for ClickHouse")
	for {
		cfg, err = config.LoadConfig(configPath)
		if err != nil {
			apexLog.Error(err.Error())
			time.Sleep(5 * time.Second)
			continue
		}
		ch := clickhouse.ClickHouse{
			Config: &cfg.ClickHouse,
		}
		if err := ch.Connect(); err != nil {
			apexLog.Error(err.Error())
			time.Sleep(5 * time.Second)
			continue
		}
		_ = ch.GetConn().Close()
		break
	}
	api := APIServer{
		c:                       c,
		configPath:              configPath,
		config:                  cfg,
		restart:                 make(chan struct{}),
		status:                  &AsyncStatus{},
		clickhouseBackupVersion: clickhouseBackupVersion,
	}
	if cfg.API.CreateIntegrationTables {
		if err := api.CreateIntegrationTables(); err != nil {
			apexLog.Error(err.Error())
		}
	}
	api.metrics = setupMetrics()

	apexLog.Infof("Starting API server on %s", api.config.API.ListenAddr)
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, os.Interrupt, syscall.SIGHUP)
	if err := api.Restart(); err != nil {
		return err
	}
	go func() {
		if err := api.updateBackupMetrics(false); err != nil {
			apexLog.Errorf("updateBackupMetrics return error: %v", err)
		}
	}()

	for {
		select {
		case <-api.restart:
			if err := api.Restart(); err != nil {
				apexLog.Errorf("Failed to restarting API server: %v", err)
				continue
			}
			apexLog.Infof("Reloaded by HTTP")
		case <-sighup:
			if err := api.Restart(); err != nil {
				apexLog.Errorf("Failed to restarting API server: %v", err)
				continue
			}
			apexLog.Info("Reloaded by SIGHUP")
		case <-sigterm:
			apexLog.Info("Stopping API server")
			return api.server.Close()
		}
	}
}

func (api *APIServer) Restart() error {
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		return err
	}
	api.metrics.NumberBackupsRemoteExpected.Set(float64(cfg.General.BackupsToKeepRemote))
	api.metrics.NumberBackupsLocalExpected.Set(float64(cfg.General.BackupsToKeepLocal))
	api.config = cfg
	server := api.setupAPIServer()
	if api.server != nil {
		_ = api.server.Close()
	}
	api.server = server
	if api.config.API.Secure {
		go func() {
			err = api.server.ListenAndServeTLS(api.config.API.CertificateFile, api.config.API.PrivateKeyFile)
			if err != nil {
				apexLog.Fatalf("ListenAndServeTLS error: %s", err.Error())
			}
		}()
		return nil
	}
	go func() {
		if err = api.server.ListenAndServe(); err != nil {
			apexLog.Fatalf("ListenAndServe error: %s", err.Error())
		}
	}()
	return nil
}

// setupAPIServer - resister API routes
func (api *APIServer) setupAPIServer() *http.Server {
	r := mux.NewRouter()
	r.Use(api.basicAuthMiddleware)
	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "", fmt.Errorf("404 Not Found"))
	})
	r.MethodNotAllowedHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "", fmt.Errorf("405 Method Not Allowed"))
	})

	r.HandleFunc("/", api.httpRootHandler).Methods("GET")
	r.HandleFunc("/", api.httpRestartHandler).Methods("POST")
	r.HandleFunc("/backup/tables", api.httpTablesHandler).Methods("GET")
	r.HandleFunc("/backup/tables/all", api.httpTablesHandler).Methods("GET")
	r.HandleFunc("/backup/list", api.httpListHandler).Methods("GET")
	r.HandleFunc("/backup/list/{where}", api.httpListHandler).Methods("GET")
	r.HandleFunc("/backup/create", api.httpCreateHandler).Methods("POST")
	r.HandleFunc("/backup/clean", api.httpCleanHandler).Methods("POST")
	r.HandleFunc("/backup/upload/{name}", api.httpUploadHandler).Methods("POST")
	r.HandleFunc("/backup/download/{name}", api.httpDownloadHandler).Methods("POST")
	r.HandleFunc("/backup/restore/{name}", api.httpRestoreHandler).Methods("POST")
	r.HandleFunc("/backup/delete/{where}/{name}", api.httpDeleteHandler).Methods("POST")
	r.HandleFunc("/backup/status", api.httpBackupStatusHandler).Methods("GET")

	r.HandleFunc("/backup/actions", api.actionsLog).Methods("GET")
	r.HandleFunc("/backup/actions", api.actions).Methods("POST")

	var routes []string
	if err := r.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		t, err := route.GetPathTemplate()
		if err != nil {
			return err
		}
		routes = append(routes, t)
		return nil
	}); err != nil {
		apexLog.Errorf("mux.Router.Walk return error: %v", err)
		return nil
	}

	api.routes = routes
	registerMetricsHandlers(r, api.config.API.EnableMetrics, api.config.API.EnablePprof)
	srv := &http.Server{
		Addr:    api.config.API.ListenAddr,
		Handler: r,
	}
	return srv
}

func (api *APIServer) basicAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		query := r.URL.Query()
		if u, exist := query["user"]; exist {
			user = u[0]
		}
		if p, exist := query["pass"]; exist {
			pass = p[0]
		}
		if (user != api.config.API.Username) || (pass != api.config.API.Password) {
			w.Header().Set("WWW-Authenticate", "Basic realm=\"Provide username and password\"")
			w.WriteHeader(http.StatusUnauthorized)
			if _, err := w.Write([]byte("401 Unauthorized\n")); err != nil {
				apexLog.Errorf("RequestWriter.Write return error: %v", err)
			}
			return
		}
		next.ServeHTTP(w, r)
	})
}

// CREATE TABLE system.backup_actions (command String, start DateTime, finish DateTime, status String, error String) ENGINE=URL('http://127.0.0.1:7171/backup/actions?user=user&pass=pass', JSONEachRow)
// INSERT INTO system.backup_actions (command) VALUES ('create backup_name')
// INSERT INTO system.backup_actions (command) VALUES ('upload backup_name')
func (api *APIServer) actions(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "", err)
		return
	}
	if len(body) == 0 {
		writeError(w, http.StatusBadRequest, "", fmt.Errorf("empty request"))
		return
	}
	lines := bytes.Split(body, []byte("\n"))
	for _, line := range lines {
		row := ActionRow{}
		if err := json.Unmarshal(line, &row); err != nil {
			writeError(w, http.StatusBadRequest, "", err)
			return
		}
		apexLog.Infof(row.Command)
		args, err := shlex.Split(row.Command)
		if err != nil {
			writeError(w, http.StatusBadRequest, "", err)
			return
		}
		command := args[0]
		switch command {
		case "create", "restore", "upload", "download", "create_remote", "restore_remote":
			if !api.config.API.AllowParallel && api.status.inProgress() {
				apexLog.Info(ErrAPILocked.Error())
				writeError(w, http.StatusLocked, row.Command, ErrAPILocked)
				return
			}
			go func() {
				start := time.Now()
				api.metrics.LastStart[command].Set(float64(start.Unix()))
				defer func() {
					api.metrics.LastDuration[command].Set(float64(time.Since(start).Nanoseconds()))
					api.metrics.LastFinish[command].Set(float64(time.Now().Unix()))
				}()

				commandId := api.status.start(row.Command)
				err := api.c.Run(append([]string{"clickhouse-backup", "-c", api.configPath}, args...))
				defer api.status.stop(commandId, err)
				if err != nil {
					api.metrics.FailedCounter[command].Inc()
					api.metrics.LastStatus[command].Set(0)
					apexLog.Error(err.Error())
					return
				}
				go func() {
					if err := api.updateBackupMetrics(command == "create" || command == "restore"); err != nil {
						apexLog.Errorf("updateBackupMetrics return error: %v", err)
					}
				}()
				api.metrics.SuccessfulCounter[command].Inc()
				api.metrics.LastStatus[command].Set(1)
			}()
			sendJSONEachRow(w, http.StatusCreated, struct {
				Status    string `json:"status"`
				Operation string `json:"operation"`
			}{
				Status:    "acknowledged",
				Operation: row.Command,
			})
			return
		case "delete":
			if !api.config.API.AllowParallel && api.status.inProgress() {
				apexLog.Info(ErrAPILocked.Error())
				writeError(w, http.StatusLocked, row.Command, ErrAPILocked)
				return
			}
			commandId := api.status.start(row.Command)
			err := api.c.Run(append([]string{"clickhouse-backup", "-c", api.configPath}, args...))
			api.status.stop(commandId, err)
			if err != nil {
				writeError(w, http.StatusBadRequest, row.Command, err)
				apexLog.Error(err.Error())
				return
			}
			apexLog.Info("OK")
			go func() {
				if err := api.updateBackupMetrics(args[1] == "local"); err != nil {
					apexLog.Errorf("updateBackupMetrics return error: %v", err)
				}
			}()
			sendJSONEachRow(w, http.StatusCreated, struct {
				Status    string `json:"status"`
				Operation string `json:"operation"`
			}{
				Status:    "ok",
				Operation: row.Command,
			})
			return
		default:
			writeError(w, http.StatusBadRequest, row.Command, fmt.Errorf("unknown command"))
			return
		}
	}
}

func (api *APIServer) actionsLog(w http.ResponseWriter, r *http.Request) {
	var last int64
	var err error
	q := r.URL.Query()
	if q.Get("last") != "" {
		last, err = strconv.ParseInt(q.Get("last"), 10, 16)
		if err != nil {
			apexLog.Warn(err.Error())
			writeError(w, http.StatusInternalServerError, "actions", err)
			return
		}
	}
	sendJSONEachRow(w, http.StatusOK, api.status.status(false, q.Get("filter"), int(last)))
}

// httpRootHandler - display API index
func (api *APIServer) httpRootHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Pragma", "no-cache")

	_, _ = fmt.Fprintln(w, "Documentation: https://github.com/AlexAkulov/clickhouse-backup#api-configuration")
	for _, r := range api.routes {
		_, _ = fmt.Fprintln(w, r)
	}
}

// httpRestart - restart API server
func (api *APIServer) httpRestartHandler(w http.ResponseWriter, _ *http.Request) {
	sendJSONEachRow(w, http.StatusCreated, struct {
		Status    string `json:"status"`
		Operation string `json:"operation"`
	}{
		Status:    "acknowledged",
		Operation: "restart",
	})
	api.restart <- struct{}{}
}

// httpTablesHandler - display list of tables
func (api *APIServer) httpTablesHandler(w http.ResponseWriter, r *http.Request) {
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	api.metrics.NumberBackupsRemoteExpected.Set(float64(cfg.General.BackupsToKeepRemote))
	api.metrics.NumberBackupsLocalExpected.Set(float64(cfg.General.BackupsToKeepLocal))
	tables, err := backup.GetTables(cfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "tables", err)
		return
	}
	if r.URL.Path != "/backup/tables/all" {
		tables := api.getTablesWithSkip(tables)
		sendJSONEachRow(w, http.StatusOK, tables)
		return
	}
	sendJSONEachRow(w, http.StatusOK, tables)

}

func (api *APIServer) getTablesWithSkip(tables []clickhouse.Table) []clickhouse.Table {
	showCounts := 0
	for _, t := range tables {
		if !t.Skip {
			showCounts++
		}
	}
	showTables := make([]clickhouse.Table, showCounts)
	showCounts = 0
	for _, t := range tables {
		if !t.Skip {
			showTables[showCounts] = t
			showCounts++
		}
	}
	return showTables
}

// httpTablesHandler - display list of all backups stored locally and remotely
// CREATE TABLE system.backup_list (name String, created DateTime, size Int64, location String, desc String) ENGINE=URL('http://127.0.0.1:7171/backup/list?user=user&pass=pass', JSONEachRow)
// ??? INSERT INTO system.backup_list (name,location) VALUES ('backup_name', 'remote') - upload backup
// ??? INSERT INTO system.backup_list (name) VALUES ('backup_name') - create backup
func (api *APIServer) httpListHandler(w http.ResponseWriter, r *http.Request) {
	type backupJSON struct {
		Name           string `json:"name"`
		Created        string `json:"created"`
		Size           uint64 `json:"size,omitempty"`
		Location       string `json:"location"`
		RequiredBackup string `json:"required"`
		Desc           string `json:"desc"`
	}
	backupsJSON := make([]backupJSON, 0)
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	api.metrics.NumberBackupsRemoteExpected.Set(float64(cfg.General.BackupsToKeepRemote))
	api.metrics.NumberBackupsLocalExpected.Set(float64(cfg.General.BackupsToKeepLocal))
	vars := mux.Vars(r)
	where, wherePresent := vars["where"]

	if where == "local" || !wherePresent {
		localBackups, _, err := backup.GetLocalBackups(cfg, nil)
		if err != nil && !os.IsNotExist(err) {
			writeError(w, http.StatusInternalServerError, "list", err)
			return
		}
		for _, b := range localBackups {
			description := b.DataFormat
			if b.Legacy {
				description = "old-format"
			}
			if b.Broken != "" {
				description = b.Broken
			}
			backupsJSON = append(backupsJSON, backupJSON{
				Name:           b.BackupName,
				Created:        b.CreationDate.Format(APITimeFormat),
				Size:           b.DataSize + b.MetadataSize,
				Location:       "local",
				RequiredBackup: b.RequiredBackup,
				Desc:           description,
			})
		}
		api.metrics.NumberBackupsLocal.Set(float64(len(localBackups)))
	}
	if cfg.General.RemoteStorage != "none" && (where == "remote" || !wherePresent) {
		remoteBackups, err := backup.GetRemoteBackups(cfg, true)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "list", err)
			return
		}
		for i, b := range remoteBackups {
			description := b.DataFormat
			if b.Legacy {
				description = "old-format"
			}
			if b.Broken != "" {
				description = b.Broken
			}
			backupsJSON = append(backupsJSON, backupJSON{
				Name:           b.BackupName,
				Created:        b.CreationDate.Format(APITimeFormat),
				Size:           b.DataSize + b.MetadataSize,
				Location:       "remote",
				RequiredBackup: b.RequiredBackup,
				Desc:           description,
			})
			if i == len(remoteBackups)-1 {
				api.metrics.LastBackupSizeRemote.Set(float64(b.DataSize + b.MetadataSize + b.ConfigSize + b.RBACSize))
			}
		}
		api.metrics.NumberBackupsRemote.Set(float64(len(remoteBackups)))
	}
	sendJSONEachRow(w, http.StatusOK, backupsJSON)
}

// httpCreateHandler - create a backup
func (api *APIServer) httpCreateHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && api.status.inProgress() {
		apexLog.Info(ErrAPILocked.Error())
		writeError(w, http.StatusLocked, "create", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "create", err)
		return
	}
	api.metrics.NumberBackupsRemoteExpected.Set(float64(cfg.General.BackupsToKeepRemote))
	api.metrics.NumberBackupsLocalExpected.Set(float64(cfg.General.BackupsToKeepLocal))
	tablePattern := ""
	partitionsToBackup := make([]string, 0)
	backupName := backup.NewBackupName()
	schemaOnly := false
	rbacOnly := false
	configsOnly := false
	fullCommand := "create"
	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if partitions, exist := query["partitions"]; exist {
		partitionsToBackup = strings.Split(partitions[0], ",")
		fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, partitions)
	}
	if schema, exist := query["schema"]; exist {
		schemaOnly, _ = strconv.ParseBool(schema[0])
		if schemaOnly {
			fullCommand = fmt.Sprintf("%s --schema", fullCommand)
		}
	}
	if rbac, exist := query["rbac"]; exist {
		rbacOnly, _ = strconv.ParseBool(rbac[0])
		if rbacOnly {
			fullCommand = fmt.Sprintf("%s --rbac", fullCommand)
		}
	}
	if configs, exist := query["configs"]; exist {
		configsOnly, _ = strconv.ParseBool(configs[0])
		if configsOnly {
			fullCommand = fmt.Sprintf("%s --configs", fullCommand)
		}
	}
	if name, exist := query["name"]; exist {
		backupName = name[0]
		fullCommand = fmt.Sprintf("%s %s", fullCommand, backupName)
	}

	go func() {
		commandId := api.status.start(fullCommand)
		start := time.Now()
		api.metrics.LastStart["create"].Set(float64(start.Unix()))
		defer func() {
			api.metrics.LastDuration["create"].Set(float64(time.Since(start).Nanoseconds()))
			api.metrics.LastFinish["create"].Set(float64(time.Now().Unix()))
		}()
		err := backup.CreateBackup(cfg, backupName, tablePattern, partitionsToBackup, schemaOnly, rbacOnly, configsOnly, api.clickhouseBackupVersion)
		defer api.status.stop(commandId, err)
		if err != nil {
			api.metrics.FailedCounter["create"].Inc()
			api.metrics.LastStatus["create"].Set(0)
			apexLog.Errorf("CreateBackup error: %+v\n", err)
			return
		}
		if err := api.updateBackupMetrics(true); err != nil {
			apexLog.Errorf("updateBackupMetrics return error: %v", err)
		}
		api.metrics.SuccessfulCounter["create"].Inc()
		api.metrics.LastStatus["create"].Set(1)
	}()
	sendJSONEachRow(w, http.StatusCreated, struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
	}{
		Status:     "acknowledged",
		Operation:  "create",
		BackupName: backupName,
	})
}

// httpCleanHandler - clean ./shadow directory
func (api *APIServer) httpCleanHandler(w http.ResponseWriter, _ *http.Request) {
	commandId := api.status.start("clean")
	err := backup.Clean(api.config)
	api.status.stop(commandId, err)
	if err != nil {
		log.Printf("Clean error: %+v\n", err)
		writeError(w, http.StatusInternalServerError, "clean", err)
		return
	}
	sendJSONEachRow(w, http.StatusOK, struct {
		Status    string `json:"status"`
		Operation string `json:"operation"`
	}{
		Status:    "success",
		Operation: "clean",
	})
}

// httpUploadHandler - upload a backup to remote storage
func (api *APIServer) httpUploadHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && api.status.inProgress() {
		apexLog.Info(ErrAPILocked.Error())
		writeError(w, http.StatusLocked, "upload", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "upload", err)
		return
	}
	api.metrics.NumberBackupsRemoteExpected.Set(float64(cfg.General.BackupsToKeepRemote))
	api.metrics.NumberBackupsLocalExpected.Set(float64(cfg.General.BackupsToKeepLocal))
	vars := mux.Vars(r)
	query := r.URL.Query()
	diffFrom := ""
	diffFromRemote := ""
	name := vars["name"]
	tablePattern := ""
	partitionsToBackup := make([]string, 0)
	schemaOnly := false
	fullCommand := "upload"

	if df, exist := query["diff-from"]; exist {
		diffFrom = df[0]
		fullCommand = fmt.Sprintf("%s --diff-from=\"%s\"", fullCommand, diffFrom)
	}
	if df, exist := query["diff-from-remote"]; exist {
		diffFromRemote = df[0]
		fullCommand = fmt.Sprintf("%s --diff-from-remote=\"%s\"", fullCommand, diffFromRemote)
	}
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if partitions, exist := query["partitions"]; exist {
		partitionsToBackup = strings.Split(partitions[0], ",")
		fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, partitions)
	}
	if schema, exist := query["schema"]; exist {
		schemaOnly, _ = strconv.ParseBool(schema[0])
		fullCommand += " --schema"
	}
	fullCommand = fmt.Sprint(fullCommand, " ", name)

	go func() {
		commandId := api.status.start(fullCommand)
		start := time.Now()
		api.metrics.LastStart["upload"].Set(float64(start.Unix()))
		defer func() {
			api.metrics.LastDuration["upload"].Set(float64(time.Since(start).Nanoseconds()))
			api.metrics.LastFinish["upload"].Set(float64(time.Now().Unix()))
		}()
		b := backup.NewBackuper(cfg)
		err := b.Upload(name, diffFrom, diffFromRemote, tablePattern, partitionsToBackup, schemaOnly)
		api.status.stop(commandId, err)
		if err != nil {
			apexLog.Errorf("Upload error: %+v\n", err)
			api.metrics.FailedCounter["upload"].Inc()
			api.metrics.LastStatus["upload"].Set(0)
			return
		}
		go func() {
			if err := api.updateBackupMetrics(false); err != nil {
				apexLog.Errorf("updateBackupMetrics return error: %v", err)
			}
		}()
		api.metrics.SuccessfulCounter["upload"].Inc()
		api.metrics.LastStatus["upload"].Set(1)
	}()
	sendJSONEachRow(w, http.StatusOK, struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
		BackupFrom string `json:"backup_from,omitempty"`
		Diff       bool   `json:"diff"`
	}{
		Status:     "acknowledged",
		Operation:  "upload",
		BackupName: name,
		BackupFrom: diffFrom,
		Diff:       diffFrom != "",
	})
}

// httpRestoreHandler - restore a backup from local storage
func (api *APIServer) httpRestoreHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && api.status.inProgress() {
		apexLog.Info(ErrAPILocked.Error())
		writeError(w, http.StatusLocked, "restore", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "restore", err)
		return
	}
	api.metrics.NumberBackupsRemoteExpected.Set(float64(cfg.General.BackupsToKeepRemote))
	api.metrics.NumberBackupsLocalExpected.Set(float64(cfg.General.BackupsToKeepLocal))
	vars := mux.Vars(r)
	tablePattern := ""
	partitionsToBackup := make([]string, 0)
	schemaOnly := false
	dataOnly := false
	dropTable := false
	rbacOnly := false
	configsOnly := false
	fullCommand := "restore"

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if partitions, exist := query["partitions"]; exist {
		partitionsToBackup = strings.Split(partitions[0], ",")
		fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, partitions)
	}
	if _, exist := query["schema"]; exist {
		schemaOnly = true
		fullCommand += " --schema"
	}
	if _, exist := query["data"]; exist {
		dataOnly = true
		fullCommand += " --data"
	}
	if _, exist := query["drop"]; exist {
		dropTable = true
		fullCommand += " --drop"
	}
	if _, exist := query["rm"]; exist {
		dropTable = true
		fullCommand += " --rm"
	}
	if _, exist := query["rbac"]; exist {
		rbacOnly = true
		fullCommand += " --rbac"
	}
	if _, exist := query["configs"]; exist {
		configsOnly = true
		fullCommand += " --configs"
	}

	name := vars["name"]
	fullCommand += fmt.Sprintf(" %s", name)

	go func() {
		commandId := api.status.start(fullCommand)
		start := time.Now()
		api.metrics.LastStart["restore"].Set(float64(start.Unix()))
		defer func() {
			api.metrics.LastDuration["restore"].Set(float64(time.Since(start).Nanoseconds()))
			api.metrics.LastFinish["restore"].Set(float64(time.Now().Unix()))
		}()
		err := backup.Restore(cfg, name, tablePattern, partitionsToBackup, schemaOnly, dataOnly, dropTable, rbacOnly, configsOnly)
		api.status.stop(commandId, err)
		if err != nil {
			apexLog.Errorf("Download error: %+v\n", err)
			api.metrics.FailedCounter["restore"].Inc()
			api.metrics.LastStatus["restore"].Set(0)
			return
		}
		api.metrics.SuccessfulCounter["restore"].Inc()
		api.metrics.LastStatus["restore"].Set(1)
	}()
	sendJSONEachRow(w, http.StatusOK, struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
	}{
		Status:     "acknowledged",
		Operation:  "restore",
		BackupName: name,
	})
}

// httpDownloadHandler - download a backup from remote to local storage
func (api *APIServer) httpDownloadHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && api.status.inProgress() {
		apexLog.Info(ErrAPILocked.Error())
		writeError(w, http.StatusLocked, "download", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "download", err)
		return
	}
	api.metrics.NumberBackupsRemoteExpected.Set(float64(cfg.General.BackupsToKeepRemote))
	api.metrics.NumberBackupsLocalExpected.Set(float64(cfg.General.BackupsToKeepLocal))
	vars := mux.Vars(r)
	name := vars["name"]
	query := r.URL.Query()
	tablePattern := ""
	partitionsToBackup := make([]string, 0)
	schemaOnly := false
	fullCommand := "download"

	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if partitions, exist := query["partitions"]; exist {
		partitionsToBackup = strings.Split(partitions[0], ",")
		fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, partitions)
	}
	if _, exist := query["schema"]; exist {
		schemaOnly = true
		fullCommand += " --schema"
	}
	fullCommand += fmt.Sprintf(" %s", name)

	go func() {
		commandId := api.status.start(fullCommand)
		start := time.Now()
		api.metrics.LastStart["download"].Set(float64(start.Unix()))
		defer func() {
			api.metrics.LastDuration["download"].Set(float64(time.Since(start).Nanoseconds()))
			api.metrics.LastFinish["download"].Set(float64(time.Now().Unix()))
		}()

		b := backup.NewBackuper(cfg)
		err := b.Download(name, tablePattern, partitionsToBackup, schemaOnly)
		api.status.stop(commandId, err)
		if err != nil {
			apexLog.Errorf("Download error: %+v\n", err)
			api.metrics.FailedCounter["download"].Inc()
			api.metrics.LastStatus["download"].Set(0)
			return
		}
		if err := api.updateBackupMetrics(true); err != nil {
			apexLog.Errorf("updateBackupMetrics return error: %v", err)
		}
		api.metrics.SuccessfulCounter["download"].Inc()
		api.metrics.LastStatus["download"].Set(1)
	}()
	sendJSONEachRow(w, http.StatusOK, struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
	}{
		Status:     "acknowledged",
		Operation:  "download",
		BackupName: name,
	})
}

// httpDeleteHandler - delete a backup from local or remote storage
func (api *APIServer) httpDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && api.status.inProgress() {
		apexLog.Info(ErrAPILocked.Error())
		writeError(w, http.StatusLocked, "delete", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "delete", err)
		return
	}
	api.metrics.NumberBackupsRemoteExpected.Set(float64(cfg.General.BackupsToKeepRemote))
	api.metrics.NumberBackupsLocalExpected.Set(float64(cfg.General.BackupsToKeepLocal))
	vars := mux.Vars(r)
	fullCommand := fmt.Sprintf("delete %s %s", vars["where"], vars["name"])
	commandId := api.status.start(fullCommand)

	switch vars["where"] {
	case "local":
		err = backup.RemoveBackupLocal(cfg, vars["name"], nil)
	case "remote":
		err = backup.RemoveBackupRemote(cfg, vars["name"])
	default:
		err = fmt.Errorf("backup location must be 'local' or 'remote'")
	}
	api.status.stop(commandId, err)
	if err != nil {
		apexLog.Errorf("delete backup error: %+v\n", err)
		writeError(w, http.StatusInternalServerError, "delete", err)
		return
	}
	go func() {
		if err := api.updateBackupMetrics(vars["where"] == "local"); err != nil {
			apexLog.Errorf("updateBackupMetrics return error: %v", err)
		}
	}()
	sendJSONEachRow(w, http.StatusOK, struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
		Location   string `json:"location"`
	}{
		Status:     "success",
		Operation:  "delete",
		BackupName: vars["name"],
		Location:   vars["where"],
	})
}

func (api *APIServer) httpBackupStatusHandler(w http.ResponseWriter, _ *http.Request) {
	sendJSONEachRow(w, http.StatusOK, api.status.status(true, "", 0))
}

func (api *APIServer) updateBackupMetrics(onlyLocal bool) error {
	startTime := time.Now()
	lastSizeLocal := uint64(0)
	lastSizeRemote := uint64(0)
	numberBackupsLocal := 0
	numberBackupsRemote := 0

	apexLog.Infof("Update backup metrics start (onlyLocal=%v)", onlyLocal)
	defer func() {
		apexLog.WithFields(apexLog.Fields{
			"duration":             utils.HumanizeDuration(time.Since(startTime)),
			"LastBackupSizeRemote": lastSizeRemote,
			"LastBackupSizeLocal":  lastSizeLocal,
			"NumberBackupsLocal":   numberBackupsLocal,
			"NumberBackupsRemote":  numberBackupsRemote,
		}).Info("Update backup metrics finish")
	}()
	if !api.config.API.EnableMetrics {
		return nil
	}
	localBackups, _, err := backup.GetLocalBackups(api.config, nil)
	if err != nil {
		return err
	}
	if len(localBackups) > 0 {
		numberBackupsLocal = len(localBackups)
		lastBackup := localBackups[numberBackupsLocal-1]
		lastSizeLocal = lastBackup.DataSize + lastBackup.MetadataSize + lastBackup.ConfigSize + lastBackup.RBACSize
		api.metrics.LastBackupSizeLocal.Set(float64(lastSizeLocal))
		api.metrics.NumberBackupsLocal.Set(float64(numberBackupsLocal))
	} else {
		api.metrics.LastBackupSizeLocal.Set(0)
		api.metrics.NumberBackupsLocal.Set(0)
	}
	if api.config.General.RemoteStorage == "none" || onlyLocal {
		return nil
	}
	remoteBackups, err := backup.GetRemoteBackups(api.config, false)
	if err != nil {
		return err
	}
	if len(remoteBackups) > 0 {
		numberBackupsRemote = len(remoteBackups)
		lastBackup := remoteBackups[numberBackupsRemote-1]
		lastSizeRemote = lastBackup.DataSize + lastBackup.MetadataSize + lastBackup.ConfigSize + lastBackup.RBACSize
		api.metrics.LastBackupSizeRemote.Set(float64(lastSizeRemote))
		api.metrics.NumberBackupsRemote.Set(float64(numberBackupsRemote))
	} else {
		api.metrics.LastBackupSizeRemote.Set(0)
		api.metrics.NumberBackupsRemote.Set(0)
	}
	return nil
}

func registerMetricsHandlers(r *mux.Router, enableMetrics bool, enablePprof bool) {
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		sendJSONEachRow(w, http.StatusOK, struct {
			Status string `json:"status"`
		}{
			Status: "OK",
		})
	})
	if enableMetrics {
		r.Handle("/metrics", promhttp.Handler())
	}
	if enablePprof {
		r.HandleFunc("/debug/pprof/", pprof.Index)
		r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		r.HandleFunc("/debug/pprof/profile", pprof.Profile)
		r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		r.HandleFunc("/debug/pprof/trace", pprof.Trace)
		r.Handle("/debug/pprof/block", pprof.Handler("block"))
		r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	}
}

type Metrics struct {
	SuccessfulCounter map[string]prometheus.Counter
	FailedCounter     map[string]prometheus.Counter
	LastStart         map[string]prometheus.Gauge
	LastFinish        map[string]prometheus.Gauge
	LastDuration      map[string]prometheus.Gauge
	LastStatus        map[string]prometheus.Gauge

	LastBackupSizeLocal         prometheus.Gauge
	LastBackupSizeRemote        prometheus.Gauge
	NumberBackupsRemote         prometheus.Gauge
	NumberBackupsLocal          prometheus.Gauge
	NumberBackupsRemoteExpected prometheus.Gauge
	NumberBackupsLocalExpected  prometheus.Gauge
}

// setupMetrics - resister prometheus metrics
func setupMetrics() Metrics {
	m := Metrics{}
	successfulCounter := map[string]prometheus.Counter{}
	failedCounter := map[string]prometheus.Counter{}
	lastStart := map[string]prometheus.Gauge{}
	lastFinish := map[string]prometheus.Gauge{}
	lastDuration := map[string]prometheus.Gauge{}
	lastStatus := map[string]prometheus.Gauge{}

	for _, command := range []string{"create", "upload", "download", "restore", "create_remote", "restore_remote"} {
		successfulCounter[command] = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("successful_%ss", command),
			Help:      fmt.Sprintf("Counter of successful %ss backup", command),
		})
		failedCounter[command] = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("failed_%ss", command),
			Help:      fmt.Sprintf("Counter of failed %ss backup", command),
		})
		lastStart[command] = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("last_%s_start", command),
			Help:      fmt.Sprintf("Last backup %s start timestamp", command),
		})
		lastFinish[command] = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("last_%s_finish", command),
			Help:      fmt.Sprintf("Last backup %s finish timestamp", command),
		})
		lastDuration[command] = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("last_%s_duration", command),
			Help:      fmt.Sprintf("Backup %s duration in nanoseconds", command),
		})
		lastStatus[command] = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "clickhouse_backup",
			Name:      fmt.Sprintf("last_%s_status", command),
			Help:      fmt.Sprintf("Last backup %s status: 0=failed, 1=success, 2=unknown", command),
		})
	}

	m.SuccessfulCounter = successfulCounter
	m.FailedCounter = failedCounter
	m.LastStart = lastStart
	m.LastFinish = lastFinish
	m.LastDuration = lastDuration
	m.LastStatus = lastStatus

	m.LastBackupSizeLocal = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "last_backup_size_local",
		Help:      "Last local backup size in bytes",
	})
	m.LastBackupSizeRemote = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "last_backup_size_remote",
		Help:      "Last remote backup size in bytes",
	})

	m.NumberBackupsRemote = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "number_backups_remote",
		Help:      "Number of stored remote backups",
	})

	m.NumberBackupsRemoteExpected = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "number_backups_remote_expected",
		Help:      "How many backups expected on remote storage",
	})

	m.NumberBackupsLocal = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "number_backups_local",
		Help:      "Number of stored local backups",
	})

	m.NumberBackupsLocalExpected = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "number_backups_local_expected",
		Help:      "How many backups expected on local storage",
	})

	for _, command := range []string{"create", "upload", "download", "restore", "create_remote", "restore_remote"} {
		prometheus.MustRegister(
			m.SuccessfulCounter[command],
			m.FailedCounter[command],
			m.LastStart[command],
			m.LastFinish[command],
			m.LastDuration[command],
			m.LastStatus[command],
		)
	}

	prometheus.MustRegister(
		m.LastBackupSizeLocal,
		m.LastBackupSizeRemote,
		m.NumberBackupsRemote,
		m.NumberBackupsLocal,
		m.NumberBackupsRemoteExpected,
		m.NumberBackupsLocalExpected,
	)
	m.LastStatus["create"].Set(2) // 0=failed, 1=success, 2=unknown
	m.LastStatus["upload"].Set(2)
	m.LastStatus["download"].Set(2)
	m.LastStatus["restore"].Set(2)
	m.LastStatus["create_remote"].Set(2)
	m.LastStatus["restore_remote"].Set(2)

	return m
}

func (api *APIServer) CreateIntegrationTables() error {
	apexLog.Infof("Create integration tables")
	ch := &clickhouse.ClickHouse{
		Config: &api.config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %w", err)
	}
	defer ch.Close()
	port := strings.Split(api.config.API.ListenAddr, ":")[1]
	auth := ""
	if api.config.API.Username != "" || api.config.API.Password != "" {
		params := url.Values{}
		params.Add("user", api.config.API.Username)
		params.Add("pass", api.config.API.Password)
		auth = fmt.Sprintf("?%s", params.Encode())
	}
	schema := "http"
	if api.config.API.Secure {
		schema = "https"
	}
	host := "127.0.0.1"
	if api.config.API.IntegrationTablesHost != "" {
		host = api.config.API.IntegrationTablesHost
	}
	settings := ""
	version, err := ch.GetVersion()
	if err != nil {
		return err
	}
	if version >= 21001000 {
		settings = "SETTINGS input_format_skip_unknown_fields=1"
	}
	query := fmt.Sprintf("CREATE TABLE system.backup_actions (command String, start DateTime, finish DateTime, status String, error String) ENGINE=URL('%s://%s:%s/backup/actions%s', JSONEachRow) %s", schema, host, port, auth, settings)
	if err := ch.CreateTable(clickhouse.Table{Database: "system", Name: "backup_actions"}, query, true, "", 0); err != nil {
		return err
	}
	query = fmt.Sprintf("CREATE TABLE system.backup_list (name String, created DateTime, size Int64, location String, required String, desc String) ENGINE=URL('%s://%s:%s/backup/list%s', JSONEachRow) %s", schema, host, port, auth, settings)
	if err := ch.CreateTable(clickhouse.Table{Database: "system", Name: "backup_list"}, query, true, "", 0); err != nil {
		return err
	}
	return nil
}
