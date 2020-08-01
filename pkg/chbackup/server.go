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
	"sync"
	"time"

	"github.com/google/uuid"
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
	restart chan bool
	status  AsyncStatus
	metrics Metrics
}

type AsyncStatus struct {
	active map[string]AsyncInfo
	sync.RWMutex
}

type AsyncInfo struct {
	Command string
	Name    string
	Started int64
}

func (status *AsyncStatus) start(command, name string) string {
	status.Lock()
	defer status.Unlock()
	id := uuid.New().String()
	status.active[id] = AsyncInfo{
		Command: command,
		Name:    name,
		Started: time.Now().Unix(),
	}
	return id
}

func (status *AsyncStatus) stop(id string) {
	status.Lock()
	defer status.Unlock()
	delete(status.active, id)
}

func (status *AsyncStatus) status() map[string]AsyncInfo {
	status.RLock()
	defer status.RUnlock()
	return status.active
}

type APIResult struct {
	Type    string
	Message string
}

type APIGenericResult struct {
	Type   string
	Result interface{}
}

type APIListResult struct {
	Type string
	Backup
}

type APITablesResult struct {
	Type string
	Table
}

var (
	ErrAPILocked = errors.New("Another operation is currently running")
)

// Server - expose CLI commands as REST API
func Server(config Config) error {
	api := APIServer{
		config:  config,
		lock:    semaphore.NewWeighted(1),
		restart: make(chan bool),
		status: AsyncStatus{
			map[string]AsyncInfo{},
			sync.RWMutex{},
		},
	}
	api.metrics = setupMetrics()
	// TODO: Should add a configuration check

	for {
		api.server = api.setupAPIServer(api.config)
		go func() {
			log.Printf("Starting API server on %s", api.config.API.ListenAddr)
			if err := api.server.ListenAndServe(); err != http.ErrServerClosed {
				log.Printf("Error starting API server: %v", err)
				os.Exit(1)
			}
		}()
		<-api.restart
		// TODO: Should add shutdown
		api.server.Close()
		log.Printf("Reloading config and restarting API server.")
	}
	return nil
}

// setupAPIServer - resister API routes
func (api *APIServer) setupAPIServer(config Config) *http.Server {
	r := mux.NewRouter()
	r.HandleFunc("/", httpRootHandler).Methods("GET")

	r.HandleFunc("/backup/tables", func(w http.ResponseWriter, r *http.Request) {
		httpTablesHandler(w, r, config)
	}).Methods("GET")
	r.HandleFunc("/backup/list", func(w http.ResponseWriter, r *http.Request) {
		httpListHandler(w, r, config)
	}).Methods("GET")
	r.HandleFunc("/backup/create", func(w http.ResponseWriter, r *http.Request) {
		api.httpCreateHandler(w, r, config)
	}).Methods("POST", "GET") // NOTE: these routes allow GET to support access from ClickHouse itself
	r.HandleFunc("/backup/clean", func(w http.ResponseWriter, r *http.Request) {
		api.httpCleanHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/freeze", func(w http.ResponseWriter, r *http.Request) {
		api.httpFreezeHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/upload/{name}", func(w http.ResponseWriter, r *http.Request) {
		api.httpUploadHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/download/{name}", func(w http.ResponseWriter, r *http.Request) {
		api.httpDownloadHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/restore/{name}", func(w http.ResponseWriter, r *http.Request) {
		api.httpRestoreHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/delete/{where}/{name}", func(w http.ResponseWriter, r *http.Request) {
		api.httpDeleteHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/config/default", func(w http.ResponseWriter, r *http.Request) {
		httpConfigDefaultHandler(w, r, config)
	}).Methods("GET")
	r.HandleFunc("/backup/config", func(w http.ResponseWriter, r *http.Request) {
		httpConfigHandler(w, r, config)
	}).Methods("GET")
	r.HandleFunc("/backup/config", func(w http.ResponseWriter, r *http.Request) {
		api.httpConfigUpdateHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/status", func(w http.ResponseWriter, r *http.Request) {
		api.httpBackupStatusHandler(w, r, config)
	}).Methods("GET")

	registerMetricsHandlers(r, config.API.EnableMetrics, config.API.EnablePprof)

	srv := &http.Server{
		Addr:    config.API.ListenAddr,
		Handler: r,
	}
	return srv
}

// httpRootHandler - display API index
func httpRootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, rootHtml)
}

// httpConfigDefaultHandler - display the default config. Same as CLI: clickhouse-backup default-config
func httpConfigDefaultHandler(w http.ResponseWriter, r *http.Request, c Config) {
	defaultConfig := DefaultConfig()
	d, _ := yaml.Marshal(&defaultConfig)
	out, err := json.Marshal(APIGenericResult{Type: "success", Result: string(d)})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	fmt.Fprintln(w, string(out))
}

// httpConfigDefaultHandler - display the currently running config
func httpConfigHandler(w http.ResponseWriter, r *http.Request, c Config) {
	cfg, _ := yaml.Marshal(&c)
	out, err := json.Marshal(APIGenericResult{Type: "success", Result: string(cfg)})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	fmt.Fprintln(w, string(out))
}

// httpConfigDefaultHandler - update the currently running config
func (api *APIServer) httpConfigUpdateHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	defer api.lock.Release(1)

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: fmt.Sprintf("Error parsing POST form: %v", err.Error())})
		fmt.Fprint(w, string(out))
		return
	}

	newConfig := DefaultConfig()
	if err := yaml.Unmarshal(body, &newConfig); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: fmt.Sprintf("Error parsing new config: %v", err.Error())})
		fmt.Fprint(w, string(out))
		return
	}

	if err := validateConfig(newConfig); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: fmt.Sprintf("Error validating new config: %v", err.Error())})
		fmt.Fprint(w, string(out))
		return
	}
	log.Printf("Applying new valid config.")
	api.config = *newConfig
	api.restart <- true
}

// httpTablesHandler - displaylist of tables
func httpTablesHandler(w http.ResponseWriter, r *http.Request, c Config) {
	tables, err := getTables(c)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	for _, table := range tables {
		out, err := json.Marshal(APITablesResult{"table", table})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
			fmt.Fprint(w, string(out))
			return
		}
		fmt.Fprintln(w, string(out))
	}
}

// httpTablesHandler - display list of all backups stored locally and remotely
func httpListHandler(w http.ResponseWriter, r *http.Request, c Config) {
	localBackups, err := ListLocalBackups(c)
	if err != nil && !os.IsNotExist(err) {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	backups := []APIListResult{}
	for _, backup := range localBackups {
		backups = append(backups, APIListResult{"local", backup})
	}
	if c.General.RemoteStorage != "none" {
		remoteBackups, err := getRemoteBackups(c)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
			fmt.Fprint(w, string(out))
			return
		}
		for _, backup := range remoteBackups {
			backups = append(backups, APIListResult{"remote", backup})
		}
	}

	for _, backup := range backups {
		out, err := json.Marshal(backup)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
			fmt.Fprint(w, string(out))
			return
		}
		fmt.Fprintln(w, string(out))
	}
}

// httpCreateHandler - create a backup
func (api *APIServer) httpCreateHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	defer api.lock.Release(1)
	start := time.Now()
	api.metrics.LastBackupStart.Set(float64(start.Unix()))
	defer api.metrics.LastBackupDuration.Set(float64(time.Now().Sub(start).Nanoseconds()))
	defer api.metrics.LastBackupEnd.Set(float64(time.Now().Unix()))

	tablePattern := ""
	desiredName := ""

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
	}
	if dn, exist := query["name"]; exist {
		desiredName = dn[0]
	}

	go func() {
		id := api.status.start("create", desiredName)
		defer api.status.stop(id)
		if err := CreateBackup(c, desiredName, tablePattern); err != nil {
			api.metrics.FailedBackups.Inc()
			api.metrics.LastBackupSuccess.Set(0)
			log.Printf("CreateBackup error: %v", err)
			return
		}
	}()
	out, err := json.Marshal(APIResult{Type: "success", Message: ""})
	if err != nil {
		api.metrics.FailedBackups.Inc()
		api.metrics.LastBackupSuccess.Set(0)
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprint(w, string(out))
		return
	}
	api.metrics.SuccessfulBackups.Inc()
	api.metrics.LastBackupSuccess.Set(1)
	fmt.Fprint(w, string(out))
}

// httpFreezeHandler - freeze tables
func (api *APIServer) httpFreezeHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	defer api.lock.Release(1)

	tablePattern := ""
	if err := Freeze(c, tablePattern); err != nil {
		log.Printf("Freeze error: = %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprint(w, string(out))
		return
	}
	fmt.Fprint(w, string(out))
}

// httpCleanHandler - clean ./shadow directory
func (api *APIServer) httpCleanHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	defer api.lock.Release(1)

	if err := Clean(c); err != nil {
		log.Printf("Clean error: = %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprint(w, string(out))
		return
	}
	fmt.Fprint(w, string(out))
}

// httpUploadHandler - upload a backup to remote storage
func (api *APIServer) httpUploadHandler(w http.ResponseWriter, r *http.Request, c Config) {
	vars := mux.Vars(r)
	diffFrom := ""
	query := r.URL.Query()
	if df, exist := query["diff-from"]; exist {
		diffFrom = df[0]
	}
	name := vars["name"]
	go func() {
		id := api.status.start("upload", name)
		defer api.status.stop(id)
		if err := Upload(c, name, diffFrom); err != nil {
			log.Printf("Upload error: %+v\n", err)
			return
		}
	}()
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprint(w, string(out))
		return
	}
	fmt.Fprint(w, string(out))
}

// httpRestoreHandler - restore a backup from local storage
func (api *APIServer) httpRestoreHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprint(w, string(out))
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
	if err := Restore(c, vars["name"], tablePattern, schemaOnly, dataOnly); err != nil {
		log.Printf("Download error: %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprint(w, string(out))
		return
	}
	fmt.Fprint(w, string(out))
}

// httpDownloadHandler - download a backup from remote to local storage
func (api *APIServer) httpDownloadHandler(w http.ResponseWriter, r *http.Request, c Config) {
	vars := mux.Vars(r)
	name := vars["name"]
	go func() {
		id := api.status.start("download", name)
		defer api.status.stop(id)
		if err := Download(c, name); err != nil {
			log.Printf("Download error: %+v\n", err)
			return
		}
	}()
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprint(w, string(out))
		return
	}
	fmt.Fprint(w, string(out))
}

// httpDeleteHandler - delete a backup from local or remote storage
func (api *APIServer) httpDeleteHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprint(w, string(out))
		return
	}
	defer api.lock.Release(1)

	vars := mux.Vars(r)
	switch vars["where"] {
	case "local":
		if err := RemoveBackupLocal(c, vars["name"]); err != nil {
			log.Printf("RemoveBackupLocal error: %+v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
			fmt.Fprint(w, string(out))
			return
		}
	case "remote":
		if err := RemoveBackupRemote(c, vars["name"]); err != nil {
			log.Printf("RemoveBackupRemote error: %+v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
			fmt.Fprint(w, string(out))
			return
		}
	default:
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: "Backup location must be 'local' or 'remote'."})
		fmt.Fprint(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprint(w, string(out))
		return
	}
	fmt.Fprint(w, string(out))
}

func (api *APIServer) httpBackupStatusHandler(w http.ResponseWriter, r *http.Request, c Config) {
	out, err := json.Marshal(api.status.status())
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprint(w, string(out))
		return
	}
	fmt.Fprint(w, string(out))
}

const rootHtml = `<html><body>
<h1>clickhouse-backup API</h1>
See: <a href="https://github.com/AlexAkulov/clickhouse-backup#api-configuration">https://github.com/AlexAkulov/clickhouse-backup#api-configuration</a>
</body></html>`

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
