package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/backup"

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
	c          *cli.App
	configPath string
	config     *config.APIConfig
	server     *http.Server
	restart    chan struct{}
	status     *AsyncStatus
	metrics    Metrics
	routes     []string
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

func (status *AsyncStatus) start(command string) {
	status.Lock()
	defer status.Unlock()
	status.commands = append(status.commands, ActionRow{
		Command: command,
		Start:   time.Now().Format(APITimeFormat),
		Status:  InProgressText,
	})
}

func (status *AsyncStatus) stop(err error) {
	status.Lock()
	defer status.Unlock()
	n := len(status.commands) - 1
	s := "success"
	if err != nil {
		s = "error"
		status.commands[n].Error = err.Error()
	}
	status.commands[n].Status = s
	status.commands[n].Finish = time.Now().Format(APITimeFormat)
}

func (status *AsyncStatus) status() []ActionRow {
	status.RLock()
	defer status.RUnlock()
	return status.commands
}

func (status *AsyncStatus) inProgress() bool {
	status.RLock()
	defer status.RUnlock()
	n := len(status.commands) - 1
	if n < 0 {
		return false
	}
	return status.commands[n].Status == InProgressText
}

var (
	ErrAPILocked = errors.New("another operation is currently running")
)

// Server - expose CLI commands as REST API
func Server(c *cli.App, configPath string) error {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return err
	}
	api := APIServer{
		c:          c,
		configPath: configPath,
		config:     &cfg.API,
		restart:    make(chan struct{}),
		status:     &AsyncStatus{},
	}
	api.metrics = setupMetrics()
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, os.Interrupt, syscall.SIGHUP)
	log.Printf("Starting API server on %s", api.config.ListenAddr)
	if err := api.Restart(); err != nil {
		return err
	}

	for {
		select {
		case <-api.restart:
			if err := api.Restart(); err != nil {
				log.Printf("Failed to restarting API server: %v", err)
				continue
			}
			log.Println("Reloaded by HTTP")
		case <-sighup:
			if err := api.Restart(); err != nil {
				log.Printf("Failed to restarting API server: %v", err)
				continue
			}
			log.Println("Reloaded by SYSHUP")
		case <-sigterm:
			log.Println("Stopping API server")
			return api.server.Close()
		}
	}
}

func (api *APIServer) Restart() error {
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		return err
	}
	api.config = &cfg.API
	server := api.setupAPIServer()
	if api.server != nil {
		api.server.Close()
	}
	api.server = server
	if api.config.Secure {
		go api.server.ListenAndServeTLS(api.config.CertificateFile, api.config.PrivateKeyFile)
		return nil
	}
	go api.server.ListenAndServe()
	return nil
}

// setupAPIServer - resister API routes
func (api *APIServer) setupAPIServer() *http.Server {
	r := mux.NewRouter()
	r.Use(api.basicAuthMidleware)
	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "", fmt.Errorf("404 Not Found"))
	})
	r.MethodNotAllowedHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "", fmt.Errorf("405 Method Not Allowed"))
	})

	r.HandleFunc("/", api.httpRootHandler).Methods("GET")

	r.HandleFunc("/backup/tables", api.httpTablesHandler).Methods("GET")
	r.HandleFunc("/backup/list", api.httpListHandler).Methods("GET")
	r.HandleFunc("/backup/create", api.httpCreateHandler).Methods("POST")
	r.HandleFunc("/backup/clean", api.httpCleanHandler).Methods("POST")
	r.HandleFunc("/backup/freeze", api.httpFreezeHandler).Methods("POST")
	r.HandleFunc("/backup/upload/{name}", api.httpUploadHandler).Methods("POST")
	r.HandleFunc("/backup/download/{name}", api.httpDownloadHandler).Methods("POST")
	r.HandleFunc("/backup/restore/{name}", api.httpRestoreHandler).Methods("POST")
	r.HandleFunc("/backup/delete/{where}/{name}", api.httpDeleteHandler).Methods("POST")
	r.HandleFunc("/backup/status", api.httpBackupStatusHandler).Methods("GET")

	r.HandleFunc("/backup/actions", api.actionsLog).Methods("GET")
	r.HandleFunc("/backup/actions", api.actions).Methods("POST")

	var routes []string
	r.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		t, err := route.GetPathTemplate()
		if err != nil {
			return err
		}
		routes = append(routes, t)
		return nil
	})
	api.routes = routes
	registerMetricsHandlers(r, api.config.EnableMetrics, api.config.EnablePprof)
	srv := &http.Server{
		Addr:    api.config.ListenAddr,
		Handler: r,
	}
	return srv
}

func (api *APIServer) basicAuthMidleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		query := r.URL.Query()
		if u, exist := query["user"]; exist {
			user = u[0]
		}
		if p, exist := query["pass"]; exist {
			pass = p[0]
		}
		if (user != api.config.Username) || (pass != api.config.Password) {
			w.Header().Set("WWW-Authenticate", "Basic realm=\"Provide username and password\"")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("401 Unauthorized\n"))
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
		log.Println(row.Command)
		args, err := shlex.Split(row.Command)
		if err != nil {
			writeError(w, http.StatusBadRequest, "", err)
			return
		}
		switch args[0] {
		case "create", "restore", "upload", "download", "create_remote", "restore_remote":
			if api.status.inProgress() {
				log.Println(ErrAPILocked)
				writeError(w, http.StatusLocked, row.Command, ErrAPILocked)
				return
			}
			start := time.Now()
			api.metrics.LastBackupStart.Set(float64(start.Unix()))
			defer api.metrics.LastBackupDuration.Set(float64(time.Since(start).Nanoseconds()))
			defer api.metrics.LastBackupEnd.Set(float64(time.Now().Unix()))

			go func() {
				api.status.start(row.Command)
				err := api.c.Run(append([]string{"clickhouse-backup", "-c", api.configPath}, args...))
				defer api.status.stop(err)
				if err != nil {
					api.metrics.FailedBackups.Inc()
					api.metrics.LastBackupSuccess.Set(0)
					log.Println(err)
					return
				}
			}()
			api.metrics.SuccessfulBackups.Inc()
			api.metrics.LastBackupSuccess.Set(1)
			sendJSONEachRow(w, http.StatusCreated, struct {
				Status    string `json:"status"`
				Operation string `json:"operation"`
			}{
				Status:    "acknowledged",
				Operation: row.Command,
			})
			return
		case "delete", "freeze", "clean":
			if api.status.inProgress() {
				log.Println(ErrAPILocked)
				writeError(w, http.StatusLocked, row.Command, ErrAPILocked)
				return
			}
			start := time.Now()
			api.metrics.LastBackupStart.Set(float64(start.Unix()))
			defer api.metrics.LastBackupDuration.Set(float64(time.Since(start).Nanoseconds()))
			defer api.metrics.LastBackupEnd.Set(float64(time.Now().Unix()))

			api.status.start(row.Command)
			err := api.c.Run(append([]string{"clickhouse-backup", "-c", api.configPath}, args...))
			defer api.status.stop(err)
			if err != nil {
				api.metrics.FailedBackups.Inc()
				api.metrics.LastBackupSuccess.Set(0)
				writeError(w, http.StatusBadRequest, row.Command, err)
				log.Println(err)
				return
			}
			api.metrics.SuccessfulBackups.Inc()
			api.metrics.LastBackupSuccess.Set(1)
			log.Println("OK")
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
	sendJSONEachRow(w, http.StatusOK, api.status.status())
}

// httpRootHandler - display API index
func (api *APIServer) httpRootHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Pragma", "no-cache")

	fmt.Fprintln(w, "Documentation: https://github.com/AlexAkulov/clickhouse-backup#api-configuration")
	for _, r := range api.routes {
		fmt.Fprintln(w, r)
	}
}

// httpTablesHandler - displaylist of tables
func (api *APIServer) httpTablesHandler(w http.ResponseWriter, r *http.Request) {
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	tables, err := backup.GetTables(*cfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "tables", err)
		return
	}
	sendJSONEachRow(w, http.StatusOK, tables)
}

// httpTablesHandler - display list of all backups stored locally and remotely
// CREATE TABLE system.backup_list (name String, created DateTime, size Int64, location String) ENGINE=URL('http://127.0.0.1:7171/backup/list?user=user&pass=pass', JSONEachRow)
// ??? INSERT INTO system.backup_list (name,location) VALUES ('backup_name', 'remote') - upload backup
// ??? INSERT INTO system.backup_list (name) VALUES ('backup_name') - create backup
func (api *APIServer) httpListHandler(w http.ResponseWriter, r *http.Request) {
	type backupJSON struct {
		Name     string `json:"name"`
		Created  string `json:"created"`
		Size     int64  `json:"size,omitempty"`
		Location string `json:"location"`
	}
	backupsJSON := make([]backupJSON, 0)
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	localBackups, err := backup.ListLocalBackups(*cfg)
	if err != nil && !os.IsNotExist(err) {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	for _, b := range localBackups {
		backupsJSON = append(backupsJSON, backupJSON{
			Name:     b.Name,
			Created:  b.Date.Format(APITimeFormat),
			Location: "local",
		})
	}
	if cfg.General.RemoteStorage != "none" {
		remoteBackups, err := backup.GetRemoteBackups(*cfg)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "list", err)
			return
		}
		for _, b := range remoteBackups {
			backupsJSON = append(backupsJSON, backupJSON{
				Name:     b.Name,
				Created:  b.Date.Format(APITimeFormat),
				Size:     b.Size,
				Location: "remote",
			})
		}
	}
	sendJSONEachRow(w, http.StatusOK, backupsJSON)
}

// httpCreateHandler - create a backup
func (api *APIServer) httpCreateHandler(w http.ResponseWriter, r *http.Request) {
	if api.status.inProgress() {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "create", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	start := time.Now()
	api.metrics.LastBackupStart.Set(float64(start.Unix()))
	defer api.metrics.LastBackupDuration.Set(float64(time.Since(start).Nanoseconds()))
	defer api.metrics.LastBackupEnd.Set(float64(time.Now().Unix()))

	tablePattern := ""
	params := ""
	backupName := backup.NewBackupName()

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		params = fmt.Sprintf("--table=%s ", tablePattern)
	}
	if name, exist := query["name"]; exist {
		backupName = name[0]
	}

	go func() {
		api.status.start(fmt.Sprintf("create %s%s", params, backupName))
		err := backup.CreateBackup(*cfg, backupName, tablePattern)
		defer api.status.stop(err)
		if err != nil {
			api.metrics.FailedBackups.Inc()
			api.metrics.LastBackupSuccess.Set(0)
			log.Printf("CreateBackup error: %v", err)
			return
		}
	}()
	api.metrics.SuccessfulBackups.Inc()
	api.metrics.LastBackupSuccess.Set(1)
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

// httpFreezeHandler - freeze tables
func (api *APIServer) httpFreezeHandler(w http.ResponseWriter, r *http.Request) {
	if api.status.inProgress() {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "freeze", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	query := r.URL.Query()
	tablePattern := ""
	params := ""
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		params = fmt.Sprintf("--tables=%s", tablePattern)
	}
	api.status.start(fmt.Sprintf("freeze %s", params))

	if err := backup.Freeze(*cfg, tablePattern); err != nil {
		log.Printf("Freeze error: = %+v\n", err)
		writeError(w, http.StatusInternalServerError, "freeze", err)
		return
	}
	sendJSONEachRow(w, http.StatusOK, struct {
		Status    string `json:"status"`
		Operation string `json:"operation"`
	}{
		Status:    "success",
		Operation: "freeze",
	})
}

// httpCleanHandler - clean ./shadow directory
func (api *APIServer) httpCleanHandler(w http.ResponseWriter, r *http.Request) {
	if api.status.inProgress() {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "clean", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	api.status.start("clean")
	err = backup.Clean(*cfg)
	api.status.stop(err)
	if err != nil {
		log.Printf("Clean error: = %+v\n", err)
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
	if api.status.inProgress() {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "upload", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "upload", err)
		return
	}
	vars := mux.Vars(r)
	diffFrom := ""
	query := r.URL.Query()
	params := ""
	if df, exist := query["diff-from"]; exist {
		diffFrom = df[0]
		params = fmt.Sprintf("--diff-from=%s ", query["diff-from"])
	}
	name := vars["name"]
	go func() {
		api.status.start(fmt.Sprintf("upload %s%s", params, name))
		err := backup.Upload(*cfg, name, diffFrom)
		api.status.stop(err)
		if err != nil {
			log.Printf("Upload error: %+v\n", err)
			return
		}
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
	if api.status.inProgress() {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "restore", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "restore", err)
		return
	}
	vars := mux.Vars(r)
	tablePattern := ""
	schemaOnly := false
	dataOnly := false
	dropTable := false
	params := ""

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		params = fmt.Sprintf("--table=%s ", tablePattern)
	}
	if _, exist := query["schema"]; exist {
		schemaOnly = true
		params += "--schema "
	}
	if _, exist := query["data"]; exist {
		dataOnly = true
		params += "--data "
	}
	if _, exist := query["drop"]; exist {
		dropTable = true
		params += "--drop "
	}
	if _, exist := query["rm"]; exist {
		dropTable = true
		params += "--rm "
	}
	api.status.start(fmt.Sprintf("restore %s%s", params, tablePattern))
	err = backup.Restore(*cfg, vars["name"], tablePattern, schemaOnly, dataOnly, dropTable)
	api.status.stop(err)
	if err != nil {
		log.Printf("Download error: %+v\n", err)
		writeError(w, http.StatusInternalServerError, "restore", err)
		return
	}
	sendJSONEachRow(w, http.StatusOK, struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
	}{
		Status:     "success",
		Operation:  "restore",
		BackupName: vars["name"],
	})
}

// httpDownloadHandler - download a backup from remote to local storage
func (api *APIServer) httpDownloadHandler(w http.ResponseWriter, r *http.Request) {
	if api.status.inProgress() {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "download", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "download", err)
		return
	}
	vars := mux.Vars(r)
	name := vars["name"]
	go func() {
		api.status.start(fmt.Sprintf("download %s", name))
		err := backup.Download(*cfg, name)
		api.status.stop(err)
		if err != nil {
			log.Printf("Download error: %+v\n", err)
			return
		}
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
	if api.status.inProgress() {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "delete", ErrAPILocked)
		return
	}
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "delete", err)
		return
	}
	vars := mux.Vars(r)
	api.status.start(fmt.Sprintf("delete %s %s", vars["where"], vars["name"]))
	switch vars["where"] {
	case "local":
		err = backup.RemoveBackupLocal(*cfg, vars["name"])
	case "remote":
		err = backup.RemoveBackupRemote(*cfg, vars["name"])
	default:
		err = fmt.Errorf("backup location must be 'local' or 'remote'")
	}

	api.status.stop(err)
	if err != nil {
		log.Printf("delete backup error: %+v\n", err)
		writeError(w, http.StatusInternalServerError, "delete", err)
		return
	}
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

func (api *APIServer) httpBackupStatusHandler(w http.ResponseWriter, r *http.Request) {
	sendJSONEachRow(w, http.StatusOK, api.status.status())
}

func registerMetricsHandlers(r *mux.Router, enablemetrics bool, enablepprof bool) {
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		sendJSONEachRow(w, http.StatusOK, struct {
			Status string `json:"status"`
		}{
			Status: "OK",
		})
	})
	if enablemetrics {
		r.Handle("/metrics", promhttp.Handler())
	}
	if enablepprof {
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
	LastBackupSuccess  prometheus.Gauge
	LastBackupStart    prometheus.Gauge
	LastBackupEnd      prometheus.Gauge
	LastBackupDuration prometheus.Gauge
	SuccessfulBackups  prometheus.Counter
	FailedBackups      prometheus.Counter
}

// setupMetrics - resister prometheus metrics
func setupMetrics() Metrics {
	m := Metrics{}
	m.LastBackupDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "last_backup_duration",
		Help:      "Backup duration in nanoseconds.",
	})
	m.LastBackupSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "last_backup_success",
		Help:      "Last backup success boolean: 0=failed, 1=success, 2=unknown.",
	})
	m.LastBackupStart = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "last_backup_start",
		Help:      "Last backup start timestamp.",
	})
	m.LastBackupEnd = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "clickhouse_backup",
		Name:      "last_backup_end",
		Help:      "Last backup end timestamp.",
	})
	m.SuccessfulBackups = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "clickhouse_backup",
		Name:      "successful_backups",
		Help:      "Number of Successful Backups.",
	})
	m.FailedBackups = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "clickhouse_backup",
		Name:      "failed_backups",
		Help:      "Number of Failed Backups.",
	})
	prometheus.MustRegister(
		m.LastBackupDuration,
		m.LastBackupStart,
		m.LastBackupEnd,
		m.LastBackupSuccess,
		m.SuccessfulBackups,
		m.FailedBackups,
	)
	m.LastBackupSuccess.Set(2) // 0=failed, 1=success, 2=unknown
	return m
}
