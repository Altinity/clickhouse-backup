package chbackup

import (
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

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/semaphore"
	yaml "gopkg.in/yaml.v2"
)

type APIServer struct {
	config  Config
	lock    *semaphore.Weighted
	server  *http.Server
	restart chan struct{}
	status  *AsyncStatus
	metrics Metrics
	routes  []string
}

type AsyncStatus struct {
	commands []CommandInfo
	sync.RWMutex
}

type CommandInfo struct {
	Command    string `json:"command"`
	BackupName string `json:"backup_name"`
	Status     string `json:"status"`
	Progress   string `json:"progress,omitempty"`
	Start      string `json:"start,omitempty"`
	Finish     string `json:"finish,omitempty"`
	Error      string `json:"error,omitempty"`
}

func (status *AsyncStatus) start(command string, backupName string) {
	status.Lock()
	defer status.Unlock()
	status.commands = append(status.commands, CommandInfo{
		Command:    command,
		BackupName: backupName,
		Start:      time.Now().Format(BackupTimeFormat),
		Status:     "executing",
	})
}

func (status *AsyncStatus) stop(err error) {
	status.Lock()
	defer status.Unlock()
	s := "success"
	if err != nil {
		s = "error"
		status.commands[len(status.commands)-1].Error = err.Error()
	}
	status.commands[len(status.commands)-1].Status = s
	status.commands[len(status.commands)-1].Finish = time.Now().Format(BackupTimeFormat)
}

func (status *AsyncStatus) status() *CommandInfo {
	status.RLock()
	defer status.RUnlock()
	if len(status.commands) == 0 {
		return nil
	}
	return &status.commands[len(status.commands)-1]
}

var (
	ErrAPILocked = errors.New("another operation is currently running")
)

// Server - expose CLI commands as REST API
func Server(config Config) error {
	api := APIServer{
		config:  config,
		lock:    semaphore.NewWeighted(1),
		restart: make(chan struct{}),
		status:  &AsyncStatus{},
	}
	api.metrics = setupMetrics()
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, os.Interrupt, syscall.SIGHUP)

	for {
		api.server = api.setupAPIServer(api.config)
		go func() {
			log.Printf("Starting API server on %s", api.config.API.ListenAddr)
			if err := api.server.ListenAndServe(); err != http.ErrServerClosed {
				log.Printf("error starting API server: %v", err)
				os.Exit(1)
			}
		}()
		select {
		case <-api.restart:
			log.Println("Reloading config and restarting API server")
			api.server.Close()
			continue
		case <-sighup:
			log.Println("Reloading config and restarting API server")
			api.server.Close()
			continue
		case <-sigterm:
			log.Println("Stopping API server")
			return api.server.Close()
		}
	}
}

// setupAPIServer - resister API routes
func (api *APIServer) setupAPIServer(config Config) *http.Server {
	r := mux.NewRouter()
	r.HandleFunc("/", api.httpRootHandler).Methods("GET")

	r.HandleFunc("/backup/tables", api.httpTablesHandler).Methods("GET")
	r.HandleFunc("/backup/list", api.httpListHandler).Methods("GET")
	r.HandleFunc("/backup/create", api.httpCreateHandler).Methods("POST", "GET") // NOTE: these routes allow GET to support access from ClickHouse itself
	r.HandleFunc("/backup/clean", api.httpCleanHandler).Methods("POST", "GET")
	r.HandleFunc("/backup/freeze", api.httpFreezeHandler).Methods("POST", "GET")
	r.HandleFunc("/backup/upload/{name}", api.httpUploadHandler).Methods("POST", "GET")
	r.HandleFunc("/backup/download/{name}", api.httpDownloadHandler).Methods("POST", "GET")
	r.HandleFunc("/backup/restore/{name}", api.httpRestoreHandler).Methods("POST", "GET")
	r.HandleFunc("/backup/delete/{where}/{name}", api.httpDeleteHandler).Methods("POST", "GET")
	r.HandleFunc("/backup/config/default", httpConfigDefaultHandler).Methods("GET")
	r.HandleFunc("/backup/config", api.httpConfigHandler).Methods("GET")
	r.HandleFunc("/backup/config", api.httpConfigUpdateHandler).Methods("POST")
	r.HandleFunc("/backup/status", api.httpBackupStatusHandler).Methods("GET")
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
	registerMetricsHandlers(r, config.API.EnableMetrics, config.API.EnablePprof)

	srv := &http.Server{
		Addr:    config.API.ListenAddr,
		Handler: r,
	}
	return srv
}

// httpRootHandler - display API index
func (api *APIServer) httpRootHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "Documentation: https://github.com/AlexAkulov/clickhouse-backup#api-configuration")
	for _, r := range api.routes {
		fmt.Fprintln(w, r)
	}
}

// httpConfigDefaultHandler - display the default config. Same as CLI: clickhouse-backup default-config
func httpConfigDefaultHandler(w http.ResponseWriter, r *http.Request) {
	defaultConfig := DefaultConfig()
	out, err := json.Marshal(defaultConfig)
	if err != nil {
		log.Println(err)
		writeError(w, http.StatusInternalServerError, "", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(out))
}

// httpConfigDefaultHandler - display the currently running config
func (api *APIServer) httpConfigHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: sanitaze passwords
	w.Header().Set("Content-Type", "application/json")
	out, err := json.Marshal(api.config)
	if err != nil {
		log.Println(err)
		writeError(w, http.StatusInternalServerError, "", err)
		return
	}
	fmt.Fprintln(w, string(out))
}

// httpConfigDefaultHandler - update the currently running config
func (api *APIServer) httpConfigUpdateHandler(w http.ResponseWriter, r *http.Request) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusServiceUnavailable, "update", ErrAPILocked)
		return
	}
	defer api.lock.Release(1)

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "update", fmt.Errorf("reading body error: %v", err))
		return
	}

	newConfig := DefaultConfig()
	if err := yaml.Unmarshal(body, &newConfig); err != nil {
		writeError(w, http.StatusBadRequest, "update", fmt.Errorf("error parsing new config: %v", err))
		return
	}

	if err := validateConfig(newConfig); err != nil {
		writeError(w, http.StatusBadRequest, "update", fmt.Errorf("error validating new config: %v", err))
		return
	}
	log.Printf("Applying new valid config")
	api.config = *newConfig
	api.restart <- struct{}{}
}

// httpTablesHandler - displaylist of tables
func (api *APIServer) httpTablesHandler(w http.ResponseWriter, r *http.Request) {
	tables, err := getTables(api.config)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "tables", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	out, _ := json.Marshal(tables)
	fmt.Fprintln(w, string(out))
}

// httpTablesHandler - display list of all backups stored locally and remotely
func (api *APIServer) httpListHandler(w http.ResponseWriter, r *http.Request) {
	type backup struct {
		Name     string `json:"name"`
		Created  string `json:"created"`
		Size     int64  `json:"size,omitempty"`
		Location string `json:"location"`
	}
	backups := make([]backup, 0)
	localBackups, err := ListLocalBackups(api.config)
	if err != nil && !os.IsNotExist(err) {
		writeError(w, http.StatusInternalServerError, "list", err)
		return
	}

	for _, b := range localBackups {
		backups = append(backups, backup{
			Name:     b.Name,
			Created:  b.Date.Format(BackupTimeFormat),
			Location: "local",
		})
	}
	if api.config.General.RemoteStorage != "none" {
		remoteBackups, err := getRemoteBackups(api.config)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "list", err)
			return
		}
		for _, b := range remoteBackups {
			backups = append(backups, backup{
				Name:     b.Name,
				Created:  b.Date.Format(BackupTimeFormat),
				Size:     b.Size,
				Location: "remote",
			})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	out, _ := json.Marshal(&backups)
	fmt.Fprint(w, string(out))
}

// httpCreateHandler - create a backup
func (api *APIServer) httpCreateHandler(w http.ResponseWriter, r *http.Request) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "create", ErrAPILocked)
		return
	}
	defer api.lock.Release(1)
	start := time.Now()
	api.metrics.LastBackupStart.Set(float64(start.Unix()))
	defer api.metrics.LastBackupDuration.Set(float64(time.Since(start).Nanoseconds()))
	defer api.metrics.LastBackupEnd.Set(float64(time.Now().Unix()))

	tablePattern := ""
	backupName := NewBackupName()

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
	}
	if name, exist := query["name"]; exist {
		backupName = name[0]
	}

	go func() {
		api.status.start("create", backupName)
		var err error
		defer api.status.stop(err)
		if err = CreateBackup(api.config, backupName, tablePattern); err != nil {
			api.metrics.FailedBackups.Inc()
			api.metrics.LastBackupSuccess.Set(0)
			log.Printf("CreateBackup error: %v", err)
			return
		}
	}()
	api.metrics.SuccessfulBackups.Inc()
	api.metrics.LastBackupSuccess.Set(1)
	writeSuccess(w, http.StatusCreated, "create")
}

// httpFreezeHandler - freeze tables
func (api *APIServer) httpFreezeHandler(w http.ResponseWriter, r *http.Request) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "freeze", ErrAPILocked)
		return
	}
	defer api.lock.Release(1)
	query := r.URL.Query()
	tablePattern := ""
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
	}
	if err := Freeze(api.config, tablePattern); err != nil {
		log.Printf("Freeze error: = %+v\n", err)
		writeError(w, http.StatusInternalServerError, "freeze", err)
		return
	}
	writeSuccess(w, http.StatusCreated, "freeze")
}

// httpCleanHandler - clean ./shadow directory
func (api *APIServer) httpCleanHandler(w http.ResponseWriter, r *http.Request) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "clean", ErrAPILocked)
		return
	}
	defer api.lock.Release(1)
	if err := Clean(api.config); err != nil {
		log.Printf("Clean error: = %+v\n", err)
		writeError(w, http.StatusInternalServerError, "clean", err)
		return
	}
	writeSuccess(w, http.StatusOK, "clean")
}

// httpUploadHandler - upload a backup to remote storage
func (api *APIServer) httpUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	diffFrom := ""
	query := r.URL.Query()
	if df, exist := query["diff-from"]; exist {
		diffFrom = df[0]
	}
	name := vars["name"]
	go func() {
		api.status.start("upload", name)
		var err error
		defer api.status.stop(err)
		if err = Upload(api.config, name, diffFrom); err != nil {
			log.Printf("Upload error: %+v\n", err)
			return
		}
	}()
	w.Header().Set("Content-Type", "application/json")
	out, _ := json.Marshal(struct {
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
	fmt.Fprint(w, string(out))
}

// httpRestoreHandler - restore a backup from local storage
func (api *APIServer) httpRestoreHandler(w http.ResponseWriter, r *http.Request) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "restore", ErrAPILocked)
		return
	}
	defer api.lock.Release(1)

	vars := mux.Vars(r)
	tablePattern := ""
	schemaOnly := false
	dataOnly := false

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
	}
	if _, exist := query["schema"]; exist {
		schemaOnly = true
	}
	if _, exist := query["data"]; exist {
		dataOnly = true
	}
	if err := Restore(api.config, vars["name"], tablePattern, schemaOnly, dataOnly); err != nil {
		log.Printf("Download error: %+v\n", err)
		writeError(w, http.StatusInternalServerError, "restore", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	out, _ := json.Marshal(struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
	}{
		Status:     "success",
		Operation:  "restore",
		BackupName: vars["name"],
	})
	fmt.Fprint(w, string(out))
}

// httpDownloadHandler - download a backup from remote to local storage
func (api *APIServer) httpDownloadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	go func() {
		api.status.start("download", name)
		var err error
		defer api.status.stop(err)
		if err = Download(api.config, name); err != nil {
			log.Printf("Download error: %+v\n", err)
			return
		}
	}()
	w.Header().Set("Content-Type", "application/json")
	out, _ := json.Marshal(struct {
		Status     string `json:"status"`
		Operation  string `json:"operation"`
		BackupName string `json:"backup_name"`
	}{
		Status:     "acknowledged",
		Operation:  "download",
		BackupName: name,
	})
	fmt.Fprint(w, string(out))
}

// httpDeleteHandler - delete a backup from local or remote storage
func (api *APIServer) httpDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		writeError(w, http.StatusLocked, "delete", ErrAPILocked)
		return
	}
	defer api.lock.Release(1)

	vars := mux.Vars(r)
	switch vars["where"] {
	case "local":
		if err := RemoveBackupLocal(api.config, vars["name"]); err != nil {
			log.Printf("RemoveBackupLocal error: %+v\n", err)
			writeError(w, http.StatusInternalServerError, "delete", err)
			return
		}
	case "remote":
		if err := RemoveBackupRemote(api.config, vars["name"]); err != nil {
			log.Printf("RemoveBackupRemote error: %+v\n", err)
			writeError(w, http.StatusInternalServerError, "delete", err)
			return
		}
	default:
		writeError(w, http.StatusBadRequest, "delete", fmt.Errorf("Backup location must be 'local' or 'remote'"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	out, _ := json.Marshal(struct {
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
	fmt.Fprint(w, string(out))
}

func (api *APIServer) httpBackupStatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	out, _ := json.Marshal(api.status.status())
	fmt.Fprint(w, string(out))
}

func registerMetricsHandlers(r *mux.Router, enablemetrics bool, enablepprof bool) {
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "OK")
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
