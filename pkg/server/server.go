package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/shlex"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli"

	"github.com/Altinity/clickhouse-backup/v2/pkg/backup"
	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/resumable"
	"github.com/Altinity/clickhouse-backup/v2/pkg/server/metrics"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/google/uuid"
)

type APIServer struct {
	cliApp                  *cli.App
	cliCtx                  *cli.Context
	configPath              string
	config                  *config.Config
	server                  *http.Server
	restart                 chan struct{}
	stop                    chan struct{}
	metrics                 *metrics.APIMetrics
	routes                  []string
	clickhouseBackupVersion string
}

var (
	ErrAPILocked = errors.New("another operation is currently running")
)

// Run - expose CLI commands as REST API
func Run(cliCtx *cli.Context, cliApp *cli.App, configPath string, clickhouseBackupVersion string) error {
	var (
		cfg *config.Config
		err error
	)
	log.Debug().Msg("Wait for ClickHouse")
	for {
		cfg, err = config.LoadConfig(configPath)
		if err != nil {
			log.Error().Stack().Err(err).Send()
			time.Sleep(5 * time.Second)
			continue
		}
		ch := clickhouse.ClickHouse{
			Config: &cfg.ClickHouse,
		}
		if err := ch.Connect(); err != nil {
			log.Error().Stack().Err(err).Send()
			time.Sleep(5 * time.Second)
			continue
		}
		_ = ch.GetConn().Close()
		break
	}
	api := APIServer{
		cliApp:                  cliApp,
		cliCtx:                  cliCtx,
		configPath:              configPath,
		config:                  cfg,
		restart:                 make(chan struct{}),
		clickhouseBackupVersion: clickhouseBackupVersion,
		metrics:                 metrics.NewAPIMetrics(),
		stop:                    make(chan struct{}),
	}
	api.metrics.RegisterMetrics()

	log.Info().Msgf("Starting API server %s on %s", api.cliApp.Version, api.config.API.ListenAddr)
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt, syscall.SIGTERM)
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, os.Interrupt, syscall.SIGHUP)
	if err := api.Restart(); err != nil {
		return err
	}
	if api.config.API.CompleteResumableAfterRestart {
		go func() {
			if err := api.ResumeOperationsAfterRestart(); err != nil {
				log.Error().Msgf("ResumeOperationsAfterRestart return error: %v", err)
			}
		}()
	}

	go func() {
		if metricsErr := api.UpdateBackupMetrics(context.Background(), false); metricsErr != nil {
			log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
		}
	}()

	if cliCtx.Bool("watch") {
		go api.RunWatch(cliCtx)
	}

	for {
		select {
		case <-api.restart:
			if restartErr := api.Restart(); restartErr != nil {
				log.Error().Msgf("Failed to restarting API server: %v", restartErr)
				continue
			}
			log.Info().Msgf("Reloaded by HTTP")
		case <-sighup:
			if restartErr := api.Restart(); restartErr != nil {
				log.Error().Msgf("Failed to restarting API server: %v", restartErr)
				continue
			}
			log.Info().Msg("Reloaded by SIGHUP")
		case <-sigterm:
			log.Info().Msg("Stopping API server")
			return api.Stop()
		case <-api.stop:
			log.Info().Msg("Stopping API server. Stopped from the inside of the application")
			return api.Stop()
		}
	}
}

func (api *APIServer) GetMetrics() *metrics.APIMetrics {
	return api.metrics
}

func (api *APIServer) RunWatch(cliCtx *cli.Context) {
	log.Info().Msg("Starting API Server in watch mode")
	b := backup.NewBackuper(api.config)
	commandId, _ := status.Current.Start("watch")
	err := b.Watch(cliCtx.String("watch-interval"), cliCtx.String("full-interval"), cliCtx.String("watch-backup-name-template"), "*.*", nil, nil, false, false, false, false, false, cliCtx.Bool("watch-delete-source"), api.clickhouseBackupVersion, commandId, api.GetMetrics(), cliCtx)
	api.handleWatchResponse(commandId, err)
}

// Stop cancel all running commands, @todo think about graceful period
func (api *APIServer) Stop() error {
	status.Current.CancelAll("canceled during server stop")
	return api.server.Close()
}

func (api *APIServer) Restart() error {
	_, err := api.ReloadConfig(nil, "restart")
	if err != nil {
		return err
	}
	if api.config.API.CreateIntegrationTables {
		if createErr := api.CreateIntegrationTables(); createErr != nil {
			log.Error().Err(createErr).Send()
		}
	}
	status.Current.CancelAll("canceled via API /restart")
	if api.server != nil {
		_ = api.server.Close()
	}
	server := api.registerHTTPHandlers()
	api.server = server
	if api.config.API.Secure {
		go func() {
			err = api.server.ListenAndServeTLS(api.config.API.CertificateFile, api.config.API.PrivateKeyFile)
			if err != nil {
				if errors.Is(err, http.ErrServerClosed) {
					log.Warn().Msgf("ListenAndServeTLS get signal: %s", err.Error())
				} else {
					log.Fatal().Stack().Msgf("ListenAndServeTLS error: %s", err.Error())
				}
			}
		}()
		return nil
	} else {
		go func() {
			if err = api.server.ListenAndServe(); err != nil {
				if errors.Is(err, http.ErrServerClosed) {
					log.Warn().Msgf("ListenAndServe get signal: %s", err.Error())
				} else {
					log.Fatal().Stack().Msgf("ListenAndServe error: %s", err.Error())
				}
			}
		}()
	}
	return nil
}

// registerHTTPHandlers - resister API routes
func (api *APIServer) registerHTTPHandlers() *http.Server {
	r := mux.NewRouter()
	r.Use(api.basicAuthMiddleware)
	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.writeError(w, http.StatusNotFound, r.URL.Path, fmt.Errorf("%s %s 404 Not Found", r.Method, r.URL))
	})
	r.MethodNotAllowedHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.writeError(w, http.StatusMethodNotAllowed, r.URL.Path, fmt.Errorf("405 Method %s Not Allowed", r.Method))
	})

	r.HandleFunc("/", api.httpRootHandler).Methods("GET", "HEAD")
	r.HandleFunc("/", api.httpRestartHandler).Methods("POST")
	r.HandleFunc("/restart", api.httpRestartHandler).Methods("POST", "GET")
	r.HandleFunc("/backup/version", api.httpVersionHandler).Methods("GET", "HEAD")
	r.HandleFunc("/backup/kill", api.httpKillHandler).Methods("POST", "GET")
	r.HandleFunc("/backup/watch", api.httpWatchHandler).Methods("POST", "GET")
	r.HandleFunc("/backup/tables", api.httpTablesHandler).Methods("GET")
	r.HandleFunc("/backup/tables/all", api.httpTablesHandler).Methods("GET")
	r.HandleFunc("/backup/list", api.httpListHandler).Methods("GET", "HEAD")
	r.HandleFunc("/backup/list/{where}", api.httpListHandler).Methods("GET")
	r.HandleFunc("/backup/create", api.httpCreateHandler).Methods("POST")
	r.HandleFunc("/backup/create_remote", api.httpCreateRemoteHandler).Methods("POST")
	r.HandleFunc("/backup/clean", api.httpCleanHandler).Methods("POST")
	r.HandleFunc("/backup/clean/remote_broken", api.httpCleanRemoteBrokenHandler).Methods("POST")
	r.HandleFunc("/backup/clean/local_broken", api.httpCleanLocalBrokenHandler).Methods("POST")
	r.HandleFunc("/backup/upload/{name}", api.httpUploadHandler).Methods("POST")
	r.HandleFunc("/backup/download/{name}", api.httpDownloadHandler).Methods("POST")
	r.HandleFunc("/backup/restore/{name}", api.httpRestoreHandler).Methods("POST")
	r.HandleFunc("/backup/restore_remote/{name}", api.httpRestoreRemoteHandler).Methods("POST")
	r.HandleFunc("/backup/delete/{where}/{name}", api.httpDeleteHandler).Methods("POST")
	r.HandleFunc("/backup/status", api.httpStatusHandler).Methods("GET")

	r.HandleFunc("/backup/actions", api.actionsLog).Methods("GET", "HEAD")
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
		log.Error().Msgf("mux.Router.Walk return error: %v", err)
		return nil
	}

	api.routes = routes
	api.registerMetricsHandlers(r, api.config.API.EnableMetrics, api.config.API.EnablePprof)
	srv := &http.Server{
		Addr:    api.config.API.ListenAddr,
		Handler: r,
	}
	if api.config.API.CACertFile != "" {
		caCert, err := os.ReadFile(api.config.API.CACertFile)
		if err != nil {
			log.Fatal().Stack().Msgf("api initialization error %s: %v", api.config.API.CAKeyFile, err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		srv.TLSConfig = &tls.Config{
			ClientCAs:  caCertPool,
			ClientAuth: tls.RequireAndVerifyClientCert,
		}
	}

	return srv
}

func (api *APIServer) basicAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			log.Info().Msgf("API call %s %s", r.Method, r.URL.Path)
		} else {
			log.Debug().Msgf("API call %s %s", r.Method, r.URL.Path)
		}
		user, pass, _ := r.BasicAuth()
		query := r.URL.Query()
		if u, exist := query["user"]; exist {
			user = u[0]
		}
		if p, exist := query["pass"]; exist {
			pass = p[0]
		}
		if (user != api.config.API.Username) || (pass != api.config.API.Password) {
			log.Warn().Msgf("%s %s Authorization failed %s:%s", r.Method, r.URL, user, pass)
			w.Header().Set("WWW-Authenticate", "Basic realm=\"Provide username and password\"")
			w.WriteHeader(http.StatusUnauthorized)
			if _, err := w.Write([]byte("401 Unauthorized\n")); err != nil {
				log.Error().Msgf("RequestWriter.Write return error: %v", err)
			}
			return
		}
		next.ServeHTTP(w, r)
	})
}

type actionsResultsRow struct {
	Status    string `json:"status"`
	Operation string `json:"operation"`
}

// CREATE TABLE system.backup_actions (command String, start DateTime, finish DateTime, status String, error String, operation_id String) ENGINE=URL('http://127.0.0.1:7171/backup/actions?user=user&pass=pass', JSONEachRow)
// INSERT INTO system.backup_actions (command) VALUES ('create backup_name')
// INSERT INTO system.backup_actions (command) VALUES ('upload backup_name')
func (api *APIServer) actions(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, "", err)
		return
	}
	if len(body) == 0 {
		api.writeError(w, http.StatusBadRequest, "", fmt.Errorf("empty request"))
		return
	}
	lines := bytes.Split(body, []byte("\n"))
	actionsResults := make([]actionsResultsRow, 0)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		row := status.ActionRow{}
		if err := json.Unmarshal(line, &row); err != nil {
			api.writeError(w, http.StatusBadRequest, string(line), err)
			return
		}
		log.Info().Str("version", api.cliApp.Version).Msgf("/backup/actions call: %s", row.Command)
		args, err := shlex.Split(row.Command)
		if err != nil {
			api.writeError(w, http.StatusBadRequest, "", err)
			return
		}
		command := args[0]
		switch command {
		// watch command can't be run via cli app.Run, need parsing args
		case "watch":
			actionsResults, err = api.actionsWatchHandler(w, row, args, actionsResults)
			if err != nil {
				api.writeError(w, http.StatusInternalServerError, row.Command, err)
				return
			}
		case "clean":
			actionsResults, err = api.actionsCleanHandler(w, row, command, actionsResults)
			if err != nil {
				api.writeError(w, http.StatusInternalServerError, row.Command, err)
				return
			}
		case "clean_local_broken":
			actionsResults, err = api.actionsCleanLocalBrokenHandler(w, row, command, actionsResults)
			if err != nil {
				api.writeError(w, http.StatusInternalServerError, row.Command, err)
				return
			}
		case "clean_remote_broken":
			actionsResults, err = api.actionsCleanRemoteBrokenHandler(w, row, command, actionsResults)
			if err != nil {
				api.writeError(w, http.StatusInternalServerError, row.Command, err)
				return
			}
		case "kill":
			actionsResults, err = api.actionsKillHandler(row, args, actionsResults)
			if err != nil {
				api.writeError(w, http.StatusInternalServerError, row.Command, err)
				return
			}
		case "create", "restore", "upload", "download", "create_remote", "restore_remote", "list":
			actionsResults, err = api.actionsAsyncCommandsHandler(command, args, row, actionsResults)
			if err != nil {
				api.writeError(w, http.StatusInternalServerError, row.Command, err)
				return
			}
		case "delete":
			actionsResults, err = api.actionsDeleteHandler(row, args, actionsResults)
			if err != nil {
				api.writeError(w, http.StatusInternalServerError, row.Command, err)
				return
			}
		default:
			api.writeError(w, http.StatusBadRequest, row.Command, fmt.Errorf("unknown command"))
			return
		}
	}
	api.sendJSONEachRow(w, http.StatusOK, actionsResults)
}

func (api *APIServer) actionsDeleteHandler(row status.ActionRow, args []string, actionsResults []actionsResultsRow) ([]actionsResultsRow, error) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		return actionsResults, ErrAPILocked
	}
	commandId, _ := status.Current.Start(row.Command)
	err := api.cliApp.Run(append([]string{"clickhouse-backup", "-c", api.configPath, "--command-id", strconv.FormatInt(int64(commandId), 10)}, args...))
	status.Current.Stop(commandId, err)
	if err != nil {
		return actionsResults, err
	}
	go func() {
		if metricsErr := api.UpdateBackupMetrics(context.Background(), args[1] == "local"); metricsErr != nil {
			log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
		}
	}()
	actionsResults = append(actionsResults, actionsResultsRow{
		Status:    "success",
		Operation: row.Command,
	})
	log.Info().Msg("DELETED")
	return actionsResults, nil
}

func (api *APIServer) actionsAsyncCommandsHandler(command string, args []string, row status.ActionRow, actionsResults []actionsResultsRow) ([]actionsResultsRow, error) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		return actionsResults, ErrAPILocked
	}
	// to avoid race condition between GET /backup/actions and POST /backup/actions
	commandId, _ := status.Current.Start(row.Command)
	go func() {
		err, _ := api.metrics.ExecuteWithMetrics(command, 0, func() error {
			return api.cliApp.Run(append([]string{"clickhouse-backup", "-c", api.configPath, "--command-id", strconv.FormatInt(int64(commandId), 10)}, args...))
		})
		status.Current.Stop(commandId, err)
		if err != nil {
			log.Error().Msgf("API /backup/actions error: %v", err)
			return
		}
		go func() {
			if err := api.UpdateBackupMetrics(context.Background(), command == "create" || strings.HasPrefix(command, "restore") || command == "download"); err != nil {
				log.Error().Msgf("UpdateBackupMetrics return error: %v", err)
			}
		}()
	}()
	actionsResults = append(actionsResults, actionsResultsRow{
		Status:    "acknowledged",
		Operation: row.Command,
	})
	return actionsResults, nil
}

func (api *APIServer) actionsKillHandler(row status.ActionRow, args []string, actionsResults []actionsResultsRow) ([]actionsResultsRow, error) {
	killCommand := ""
	if len(args) > 1 {
		killCommand = args[1]
	}
	commandId, _ := status.Current.Start(row.Command)
	err := status.Current.Cancel(killCommand, fmt.Errorf("canceled from API /backup/actions"))
	defer status.Current.Stop(commandId, err)
	if err != nil {
		return actionsResults, err
	}
	actionsResults = append(actionsResults, actionsResultsRow{
		Status:    "success",
		Operation: row.Command,
	})
	return actionsResults, nil
}

func (api *APIServer) actionsCleanHandler(w http.ResponseWriter, row status.ActionRow, command string, actionsResults []actionsResultsRow) ([]actionsResultsRow, error) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		log.Warn().Msg(ErrAPILocked.Error())
		return actionsResults, ErrAPILocked
	}
	commandId, ctx := status.Current.Start(command)
	cfg, err := api.ReloadConfig(w, "clean")
	if err != nil {
		status.Current.Stop(commandId, err)
		return actionsResults, err
	}
	b := backup.NewBackuper(cfg)
	err = b.Clean(ctx)
	if err != nil {
		log.Error().Msgf("actions Clean error: %v", err)
		status.Current.Stop(commandId, err)
		return actionsResults, err
	}
	log.Info().Msg("CLEANED")
	go func() {
		if metricsErr := api.UpdateBackupMetrics(context.Background(), true); metricsErr != nil {
			log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
		}
	}()
	status.Current.Stop(commandId, nil)
	actionsResults = append(actionsResults, actionsResultsRow{
		Status:    "success",
		Operation: row.Command,
	})
	return actionsResults, nil
}

func (api *APIServer) actionsCleanLocalBrokenHandler(w http.ResponseWriter, row status.ActionRow, command string, actionsResults []actionsResultsRow) ([]actionsResultsRow, error) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		return actionsResults, ErrAPILocked
	}
	commandId, _ := status.Current.Start(command)
	cfg, err := api.ReloadConfig(w, "clean_local_broken")
	if err != nil {
		status.Current.Stop(commandId, err)
		return actionsResults, err
	}
	b := backup.NewBackuper(cfg)
	err = b.CleanLocalBroken(commandId)
	if err != nil {
		log.Error().Msgf("Clean local broken error: %v", err)
		status.Current.Stop(commandId, err)
		return actionsResults, err
	}
	log.Info().Msg("CLEANED")
	go func() {
		if metricsErr := api.UpdateBackupMetrics(context.Background(), true); metricsErr != nil {
			log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
		}
	}()
	status.Current.Stop(commandId, nil)
	actionsResults = append(actionsResults, actionsResultsRow{
		Status:    "success",
		Operation: row.Command,
	})
	return actionsResults, nil
}

func (api *APIServer) actionsCleanRemoteBrokenHandler(w http.ResponseWriter, row status.ActionRow, command string, actionsResults []actionsResultsRow) ([]actionsResultsRow, error) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		return actionsResults, ErrAPILocked
	}
	commandId, _ := status.Current.Start(command)
	cfg, err := api.ReloadConfig(w, "clean_remote_broken")
	if err != nil {
		status.Current.Stop(commandId, err)
		return actionsResults, err
	}
	b := backup.NewBackuper(cfg)
	err = b.CleanRemoteBroken(commandId)
	if err != nil {
		log.Error().Msgf("Clean remote broken error: %v", err)
		status.Current.Stop(commandId, err)
		return actionsResults, err
	}
	log.Info().Msg("CLEANED")
	go func() {
		if metricsErr := api.UpdateBackupMetrics(context.Background(), false); metricsErr != nil {
			log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
		}
	}()
	status.Current.Stop(commandId, nil)
	actionsResults = append(actionsResults, actionsResultsRow{
		Status:    "success",
		Operation: row.Command,
	})
	return actionsResults, nil
}

func (api *APIServer) actionsWatchHandler(w http.ResponseWriter, row status.ActionRow, args []string, actionsResults []actionsResultsRow) ([]actionsResultsRow, error) {
	if (!api.config.API.AllowParallel && status.Current.InProgress()) || status.Current.CheckCommandInProgress(row.Command) {
		log.Warn().Err(ErrAPILocked).Send()
		return actionsResults, ErrAPILocked
	}
	cfg, err := api.ReloadConfig(w, "watch")
	if err != nil {
		return actionsResults, err
	}
	tablePattern := ""
	partitionsToBackup := make([]string, 0)
	skipProjections := make([]string, 0)
	schemaOnly := false
	backupRBAC := false
	backupConfigs := false
	backupNamedCollections := false
	skipCheckPartsColumns := false
	deleteSource := false
	watchInterval := ""
	fullInterval := ""
	watchBackupNameTemplate := ""
	fullCommand := "watch"

	simpleParseArg := func(i int, args []string, paramName string) (bool, string) {
		if strings.HasPrefix(args[i], paramName) {
			if !strings.HasPrefix(args[i], paramName+"=") {
				if i < len(args)-1 && !strings.HasPrefix(args[i+1], "--") {
					return true, strings.ReplaceAll(args[i+1], "\"", "")
				}
				if i < len(args)-1 && strings.HasPrefix(args[i+1], "--") {
					return true, ""
				}
				if i == len(args)-1 {
					return true, ""
				}
			} else {
				return true, strings.ReplaceAll(strings.SplitN(args[i], "=", 1)[1], "\"", "")
			}
		}
		return false, ""
	}
	for i := range args {
		matchParam := false
		if matchParam, watchInterval = simpleParseArg(i, args, "--watch-interval"); matchParam {
			fullCommand = fmt.Sprintf("%s --watch-interval=\"%s\"", fullCommand, watchInterval)
		}
		if matchParam, fullInterval = simpleParseArg(i, args, "--full-interval"); matchParam {
			fullCommand = fmt.Sprintf("%s --full-interval=\"%s\"", fullCommand, fullInterval)
		}
		if matchParam, watchBackupNameTemplate = simpleParseArg(i, args, "--watch-backup-name-template"); matchParam {
			fullCommand = fmt.Sprintf("%s --watch-backup-name-template=\"%s\"", fullCommand, watchBackupNameTemplate)
		}
		if matchParam, tablePattern = simpleParseArg(i, args, "--tables"); matchParam {
			fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
		}
		if matchParam, partitions := simpleParseArg(i, args, "--partitions"); matchParam {
			partitionsToBackup = append(partitionsToBackup, partitions)
			fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, partitions)
		}
		if matchParam, _ = simpleParseArg(i, args, "--schema"); matchParam {
			schemaOnly = true
			fullCommand = fmt.Sprintf("%s --schema", fullCommand)
		}
		if matchParam, _ = simpleParseArg(i, args, "--rbac"); matchParam {
			backupRBAC = true
			fullCommand = fmt.Sprintf("%s --rbac", fullCommand)
		}
		if matchParam, _ = simpleParseArg(i, args, "--configs"); matchParam {
			backupConfigs = true
			fullCommand = fmt.Sprintf("%s --configs", fullCommand)
		}
		if matchParam, _ = simpleParseArg(i, args, "--named-collections"); matchParam {
			backupNamedCollections = true
			fullCommand = fmt.Sprintf("%s --named-collections", fullCommand)
		}
		if matchParam, _ = simpleParseArg(i, args, "--skip-check-parts-columns"); matchParam {
			skipCheckPartsColumns = true
			fullCommand = fmt.Sprintf("%s --skip-check-parts-columns", fullCommand)
		}
		if matchParam, _ = simpleParseArg(i, args, "--delete-source"); matchParam {
			deleteSource = true
			fullCommand = fmt.Sprintf("%s --delete-source", fullCommand)
		}
		if matchParam, skipProjectionsFromArgs := simpleParseArg(i, args, "--skip-projections"); matchParam {
			skipProjections = append(skipProjections, skipProjectionsFromArgs)
			fullCommand = fmt.Sprintf("%s --skip-projections=%s", fullCommand, skipProjectionsFromArgs)
		}
	}

	commandId, _ := status.Current.Start(fullCommand)
	go func() {
		b := backup.NewBackuper(cfg)
		err := b.Watch(watchInterval, fullInterval, watchBackupNameTemplate, tablePattern, partitionsToBackup, skipProjections, schemaOnly, backupRBAC, backupConfigs, backupNamedCollections, skipCheckPartsColumns, deleteSource, api.clickhouseBackupVersion, commandId, api.GetMetrics(), api.cliCtx)
		api.handleWatchResponse(commandId, err)
	}()

	actionsResults = append(actionsResults, actionsResultsRow{
		Status:    "acknowledged",
		Operation: row.Command,
	})
	return actionsResults, nil
}

func (api *APIServer) handleWatchResponse(watchCommandId int, err error) {
	status.Current.Stop(watchCommandId, err)
	if err != nil {
		log.Error().Msgf("Watch error: %v", err)
	}
	if api.config.API.WatchIsMainProcess {
		// Do not stop server if 'watch' was canceled by the user command
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Info().Msg("Stopping server since watch command is stopped")
		api.stop <- struct{}{}
	}
}

func (api *APIServer) actionsLog(w http.ResponseWriter, r *http.Request) {
	var last int64
	var err error
	if r.Method == http.MethodHead {
		api.sendJSONEachRow(w, http.StatusOK, "")
		return
	}
	q := r.URL.Query()
	if q.Get("last") != "" {
		last, err = strconv.ParseInt(q.Get("last"), 10, 16)
		if err != nil {
			log.Warn().Err(err).Send()
			api.writeError(w, http.StatusInternalServerError, "actions", err)
			return
		}
	}
	api.sendJSONEachRow(w, http.StatusOK, status.Current.GetStatus(false, q.Get("filter"), int(last)))
}

// httpRootHandler - display API index
func (api *APIServer) httpRootHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Pragma", "no-cache")

	_, _ = fmt.Fprintf(w, "Version: %s\nDocumentation: https://github.com/Altinity/clickhouse-backup#api\n", api.cliApp.Version)
	for _, r := range api.routes {
		_, _ = fmt.Fprintln(w, r)
	}
}

// httpRestartHandler - restart API server
func (api *APIServer) httpRestartHandler(w http.ResponseWriter, _ *http.Request) {
	api.sendJSONEachRow(w, http.StatusCreated, struct {
		Status    string `json:"status"`
		Operation string `json:"operation"`
	}{
		Status:    "acknowledged",
		Operation: "restart",
	})
	defer func() {
		api.restart <- struct{}{}
	}()
}

// httpVersionHandler
func (api *APIServer) httpVersionHandler(w http.ResponseWriter, _ *http.Request) {
	api.sendJSONEachRow(w, http.StatusOK, struct {
		Version string `json:"version"`
	}{
		Version: api.cliApp.Version,
	})
}

// httpKillHandler - kill selected command if it InProgress
func (api *APIServer) httpKillHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	command, exists := r.URL.Query()["command"]
	if exists && len(command) > 0 {
		err = status.Current.Cancel(command[0], fmt.Errorf("canceled from API /backup/kill"))
	} else {
		err = status.Current.Cancel("", fmt.Errorf("canceled from API /backup/kill"))
	}
	if err != nil {
		api.sendJSONEachRow(w, http.StatusInternalServerError, struct {
			Status    string `json:"status"`
			Operation string `json:"operation"`
			Error     string `json:"error"`
		}{
			Status:    "error",
			Operation: "kill",
			Error:     err.Error(),
		})
	} else {
		api.sendJSONEachRow(w, http.StatusOK, struct {
			Status    string `json:"status"`
			Operation string `json:"operation"`
			Command   string `json:"command"`
		}{
			Status:    "success",
			Operation: "kill",
			Command:   command[0],
		})
	}
}

// httpTablesHandler - display list of tables
func (api *APIServer) httpTablesHandler(w http.ResponseWriter, r *http.Request) {
	cfg, err := api.ReloadConfig(w, "tables")
	if err != nil {
		return
	}
	b := backup.NewBackuper(cfg)
	q := r.URL.Query()
	var tables []clickhouse.Table
	// https://github.com/Altinity/clickhouse-backup/issues/778
	if remoteBackup, exists := api.getQueryParameter(q, "remote_backup"); exists {
		tables, err = b.GetTablesRemote(context.Background(), remoteBackup, q.Get("table"))
	} else {
		tables, err = b.GetTables(context.Background(), q.Get("table"))
	}
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, "tables", err)
		return
	}
	if r.URL.Path == "/backup/tables/all" {
		api.sendJSONEachRow(w, http.StatusOK, tables)
		return
	}
	tables = api.getTablesWithSkip(tables)
	api.sendJSONEachRow(w, http.StatusOK, tables)
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

// httpListHandler - display list of all backups stored locally and remotely, could run in parallel independent of allow_parallel=true
// CREATE TABLE system.backup_list (name String, created DateTime, size Int64, location String, desc String) ENGINE=URL('http://127.0.0.1:7171/backup/list?user=user&pass=pass', JSONEachRow)
// SELECT * FROM system.backup_list
func (api *APIServer) httpListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodHead {
		api.sendJSONEachRow(w, http.StatusOK, "")
		return
	}

	type backupJSON struct {
		Name                string `json:"name"`
		Created             string `json:"created"`
		Size                uint64 `json:"size,omitempty"`
		DataSize            uint64 `json:"data_size,omitempty"`
		ObjectDiskSize      uint64 `json:"object_disk_size,omitempty"`
		MetadataSize        uint64 `json:"metadata_size"`
		RBACSize            uint64 `json:"rbac_size,omitempty"`
		ConfigSize          uint64 `json:"config_size,omitempty"`
		NamedCollectionSize uint64 `json:"named_collection_size,omitempty"`
		CompressedSize      uint64 `json:"compressed_size,omitempty"`
		Location            string `json:"location"`
		RequiredBackup      string `json:"required"`
		Desc                string `json:"desc"`
	}
	backupsJSON := make([]backupJSON, 0)
	cfg, err := api.ReloadConfig(w, "list")
	if err != nil {
		api.writeError(w, http.StatusInternalServerError, "list", err)
		return
	}
	vars := mux.Vars(r)
	where, wherePresent := vars["where"]
	fullCommand := "list"
	if wherePresent {
		fullCommand += " " + where
	}
	commandId, ctx := status.Current.Start(fullCommand)
	defer status.Current.Stop(commandId, err)
	b := backup.NewBackuper(cfg)
	if where == "local" || !wherePresent {
		var localBackups []backup.LocalBackup
		localBackups, _, err = b.GetLocalBackups(ctx, nil)
		if err != nil && !os.IsNotExist(err) {
			api.writeError(w, http.StatusInternalServerError, "list", err)
			return
		}
		for _, item := range localBackups {
			description := item.DataFormat
			if item.Broken != "" {
				description = item.Broken
			}
			if item.Tags != "" {
				if description != "" {
					description += ", "
				}
				description += item.Tags
			}
			backupsJSON = append(backupsJSON, backupJSON{
				Name:                item.BackupName,
				Created:             item.CreationDate.In(time.Local).Format(common.TimeFormat),
				Size:                item.GetFullSize(),
				DataSize:            item.DataSize,
				ObjectDiskSize:      item.ObjectDiskSize,
				MetadataSize:        item.MetadataSize,
				RBACSize:            item.RBACSize,
				ConfigSize:          item.ConfigSize,
				NamedCollectionSize: item.NamedCollectionsSize,
				CompressedSize:      item.CompressedSize,
				Location:            "local",
				RequiredBackup:      item.RequiredBackup,
				Desc:                description,
			})
		}
		api.metrics.NumberBackupsLocal.Set(float64(len(localBackups)))
	}
	if cfg.General.RemoteStorage != "none" && (where == "remote" || !wherePresent) {
		brokenBackups := 0
		remoteBackups, err := b.GetRemoteBackups(ctx, true)
		if err != nil {
			api.writeError(w, http.StatusInternalServerError, "list", err)
			return
		}
		for i, item := range remoteBackups {
			description := item.DataFormat
			if item.Broken != "" {
				description = item.Broken
				brokenBackups++
			}
			if item.Tags != "" {
				if description != "" {
					description += ", "
				}
				description += item.Tags
			}
			fullSize := item.GetFullSize()
			backupsJSON = append(backupsJSON, backupJSON{
				Name:                item.BackupName,
				Created:             item.CreationDate.In(time.Local).Format(common.TimeFormat),
				Size:                fullSize,
				DataSize:            item.DataSize,
				ObjectDiskSize:      item.ObjectDiskSize,
				MetadataSize:        item.MetadataSize,
				RBACSize:            item.RBACSize,
				ConfigSize:          item.ConfigSize,
				NamedCollectionSize: item.NamedCollectionsSize,
				CompressedSize:      item.CompressedSize,
				Location:            "remote",
				RequiredBackup:      item.RequiredBackup,
				Desc:                description,
			})
			if i == len(remoteBackups)-1 {
				api.metrics.LastBackupSizeRemote.Set(float64(fullSize))
			}
		}
		api.metrics.NumberBackupsRemoteBroken.Set(float64(brokenBackups))
		api.metrics.NumberBackupsRemote.Set(float64(len(remoteBackups)))
	}
	api.sendJSONEachRow(w, http.StatusOK, backupsJSON)
}

// httpCreateHandler - create a backup
func (api *APIServer) httpCreateHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		api.writeError(w, http.StatusLocked, "create", ErrAPILocked)
		return
	}
	cfg, err := api.ReloadConfig(w, "create")
	if err != nil {
		return
	}
	tablePattern := ""
	diffFromRemote := ""
	partitionsToBackup := make([]string, 0)
	backupName := backup.NewBackupName()
	schemaOnly := false
	createRBAC := false
	rbacOnly := false
	createConfigs := false
	configsOnly := false
	createNamedCollections := false
	namedCollectionsOnly := false
	checkPartsColumns := true
	skipProjections := make([]string, 0)
	resume := false
	fullCommand := "create"
	query := r.URL.Query()
	operationId, _ := uuid.NewUUID()

	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if baseBackup, exists := api.getQueryParameter(query, "diff-from-remote"); exists {
		diffFromRemote = baseBackup
	}
	if partitions, exist := query["partitions"]; exist {
		partitionsToBackup = append(partitionsToBackup, partitions...)
		fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, strings.Join(partitions, "\" --partitions=\""))
	}
	if _, exist := query["schema"]; exist {
		schemaOnly = true
		fullCommand += " --schema"
	}
	if _, exist := query["rbac"]; exist {
		createRBAC = true
		fullCommand += " --rbac"
	}
	if _, exist := api.getQueryParameter(query, "rbac-only"); exist {
		rbacOnly = true
		fullCommand += " --rbac-only"
	}
	if _, exist := query["configs"]; exist {
		createConfigs = true
		fullCommand += " --configs"
	}
	if _, exist := api.getQueryParameter(query, "configs-only"); exist {
		configsOnly = true
		fullCommand += " --configs-only"
	}
	if _, exist := api.getQueryParameter(query, "named-collections"); exist {
		createNamedCollections = true
		fullCommand += " --named-collections"
	}
	if _, exist := api.getQueryParameter(query, "named-collections-only"); exist {
		namedCollectionsOnly = true
		fullCommand += " --named-collections-only"
	}

	if _, exist := api.getQueryParameter(query, "skip-check-parts-columns"); exist {
		checkPartsColumns = true
		fullCommand += " --skip-check-parts-columns"
	}

	if skipProjectionsFromQuery, exist := api.getQueryParameter(query, "skip-projections"); exist {
		skipProjections = append(skipProjections, skipProjectionsFromQuery)
		fullCommand += " --skip-projections=" + strings.Join(skipProjections, ",")
	}

	if _, exist := query["resume"]; exist {
		resume = true
		fullCommand += " --resume"
	}

	if name, exist := query["name"]; exist {
		backupName = utils.CleanBackupNameRE.ReplaceAllString(name[0], "")
		fullCommand = fmt.Sprintf("%s %s", fullCommand, backupName)
	}

	callback, err := parseCallback(query)
	if err != nil {
		log.Error().Err(err).Send()
		api.writeError(w, http.StatusBadRequest, "create", err)
		return
	}

	commandId, _ := status.Current.StartWithOperationId(fullCommand, operationId.String())
	go func() {
		err, _ := api.metrics.ExecuteWithMetrics("create", 0, func() error {
			b := backup.NewBackuper(cfg)
			return b.CreateBackup(backupName, diffFromRemote, tablePattern, partitionsToBackup, schemaOnly, createRBAC, rbacOnly, createConfigs, configsOnly, createNamedCollections, namedCollectionsOnly, checkPartsColumns, skipProjections, resume, api.clickhouseBackupVersion, commandId)
		})
		if err != nil {
			log.Error().Msgf("API /backup/create error: %v", err)
			status.Current.Stop(commandId, err)
			api.errorCallback(context.Background(), err, operationId.String(), callback)
			return
		}
		go func() {
			if metricsErr := api.UpdateBackupMetrics(context.Background(), true); metricsErr != nil {
				log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
			}
		}()

		status.Current.Stop(commandId, nil)
		api.successCallback(context.Background(), operationId.String(), callback)
	}()
	api.sendJSONEachRow(w, http.StatusCreated, struct {
		Status      string `json:"status"`
		Operation   string `json:"operation"`
		BackupName  string `json:"backup_name"`
		OperationId string `json:"operation_id"`
	}{
		Status:      "acknowledged",
		Operation:   "create",
		BackupName:  backupName,
		OperationId: operationId.String(),
	})
}

// httpCreateRemoteHandler - create and upload a backup
func (api *APIServer) httpCreateRemoteHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		api.writeError(w, http.StatusLocked, "create_remote", ErrAPILocked)
		return
	}
	cfg, err := api.ReloadConfig(w, "create_remote")
	if err != nil {
		return
	}
	tablePattern := ""
	diffFrom := ""
	diffFromRemote := ""
	partitionsToBackup := make([]string, 0)
	backupName := backup.NewBackupName()
	schemaOnly := false
	backupRBAC := false
	rbacOnly := false
	backupConfigs := false
	configsOnly := false
	backupNamedCollections := false
	namedCollectionsOnly := false
	skipCheckPartsColumns := false
	skipProjections := make([]string, 0)
	deleteSource := false
	resume := false
	fullCommand := "create_remote"
	query := r.URL.Query()
	operationId, _ := uuid.NewUUID()

	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if df, exist := api.getQueryParameter(query, "diff-from"); exist {
		diffFrom = df
		fullCommand = fmt.Sprintf("%s --diff-from=\"%s\"", fullCommand, diffFrom)
	}
	if baseBackup, exists := api.getQueryParameter(query, "diff-from-remote"); exists {
		diffFromRemote = baseBackup
		fullCommand = fmt.Sprintf("%s --diff-from-remote=\"%s\"", fullCommand, diffFromRemote)
	}
	if partitions, exist := query["partitions"]; exist {
		partitionsToBackup = append(partitionsToBackup, partitions...)
		fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, strings.Join(partitions, "\" --partitions=\""))
	}
	if _, exist := query["schema"]; exist {
		schemaOnly = true
		fullCommand += " --schema"
	}
	if _, exist := query["rbac"]; exist {
		backupRBAC = true
		fullCommand += " --rbac"
	}
	if _, exist := api.getQueryParameter(query, "rbac-only"); exist {
		rbacOnly = true
		fullCommand += " --rbac-only"
	}
	if _, exist := query["configs"]; exist {
		backupConfigs = true
		fullCommand += " --configs"
	}
	if _, exist := api.getQueryParameter(query, "configs-only"); exist {
		configsOnly = true
		fullCommand += " --configs-only"
	}
	if _, exist := api.getQueryParameter(query, "named-collections"); exist {
		backupNamedCollections = true
		fullCommand += " --named-collections"
	}
	if _, exist := api.getQueryParameter(query, "named-collections-only"); exist {
		namedCollectionsOnly = true
		fullCommand += " --named-collections-only"
	}

	if _, exist := api.getQueryParameter(query, "skip-check-parts-columns"); exist {
		skipCheckPartsColumns = true
		fullCommand += " --skip-check-parts-columns"
	}

	if skipProjectionsFromQuery, exist := api.getQueryParameter(query, "skip-projections"); exist {
		skipProjections = append(skipProjections, skipProjectionsFromQuery)
		fullCommand += " --skip-projections=" + strings.Join(skipProjections, ",")
	}
	if _, exist := api.getQueryParameter(query, "delete-source"); exist {
		deleteSource = true
		fullCommand += " --delete-source"
	}
	if _, exist := query["resume"]; exist {
		resume = true
		fullCommand += " --resume"
	}
	if name, exist := query["name"]; exist {
		backupName = utils.CleanBackupNameRE.ReplaceAllString(name[0], "")
		fullCommand = fmt.Sprintf("%s %s", fullCommand, backupName)
	}

	callback, err := parseCallback(query)
	if err != nil {
		log.Error().Err(err).Send()
		api.writeError(w, http.StatusBadRequest, "create_remote", err)
		return
	}

	commandId, _ := status.Current.Start(fullCommand)
	go func() {
		err, _ := api.metrics.ExecuteWithMetrics("create_remote", 0, func() error {
			b := backup.NewBackuper(cfg)
			return b.CreateToRemote(backupName, deleteSource, diffFrom, diffFromRemote, tablePattern, partitionsToBackup, skipProjections, schemaOnly, backupRBAC, rbacOnly, backupConfigs, configsOnly, backupNamedCollections, namedCollectionsOnly, skipCheckPartsColumns, resume, api.clickhouseBackupVersion, commandId)
		})
		if err != nil {
			log.Error().Msgf("API /backup/create_remote error: %v", err)
			status.Current.Stop(commandId, err)
			api.errorCallback(context.Background(), err, operationId.String(), callback)
			return
		}
		go func() {
			if metricsErr := api.UpdateBackupMetrics(context.Background(), false); metricsErr != nil {
				log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
			}
		}()

		status.Current.Stop(commandId, nil)
		api.successCallback(context.Background(), operationId.String(), callback)
	}()
	api.sendJSONEachRow(w, http.StatusCreated, struct {
		Status      string `json:"status"`
		Operation   string `json:"operation"`
		BackupName  string `json:"backup_name"`
		OperationId string `json:"operation_id"`
	}{
		Status:      "acknowledged",
		Operation:   "create_remote",
		BackupName:  backupName,
		OperationId: operationId.String(),
	})
}

// httpWatchHandler - run watch command go routine, can't run the same watch command twice
func (api *APIServer) httpWatchHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		api.writeError(w, http.StatusLocked, "watch", ErrAPILocked)
		return
	}
	cfg, err := api.ReloadConfig(w, "watch")
	if err != nil {
		return
	}
	tablePattern := ""
	partitionsToBackup := make([]string, 0)
	skipProjections := make([]string, 0)
	schemaOnly := false
	backupRBAC := false
	backupConfigs := false
	backupNamedCollections := false
	skipCheckPartsColumns := false
	deleteSource := false
	watchInterval := ""
	fullInterval := ""
	watchBackupNameTemplate := ""
	fullCommand := "watch"
	query := r.URL.Query()
	if interval, exist := api.getQueryParameter(query, "watch_interval"); exist {
		watchInterval = interval
		fullCommand = fmt.Sprintf("%s --watch-interval=\"%s\"", fullCommand, watchInterval)
	}
	if interval, exist := api.getQueryParameter(query, "full_interval"); exist {
		fullInterval = interval
		fullCommand = fmt.Sprintf("%s --full-interval=\"%s\"", fullCommand, fullInterval)
	}
	if template, exist := api.getQueryParameter(query, "watch_backup_name_template"); exist {
		watchBackupNameTemplate = template
		fullCommand = fmt.Sprintf("%s --watch-backup-name-template=\"%s\"", fullCommand, watchBackupNameTemplate)
	}
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if partitions, exist := query["partitions"]; exist {
		partitionsToBackup = append(partitionsToBackup, partitions...)
		fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, strings.Join(partitions, "\" --partitions=\""))
	}
	if schema, exist := query["schema"]; exist {
		schemaOnly, _ = strconv.ParseBool(schema[0])
		if schemaOnly {
			fullCommand = fmt.Sprintf("%s --schema", fullCommand)
		}
	}
	if rbac, exist := query["rbac"]; exist {
		backupRBAC, _ = strconv.ParseBool(rbac[0])
		if backupRBAC {
			fullCommand = fmt.Sprintf("%s --rbac", fullCommand)
		}
	}
	if configs, exist := query["configs"]; exist {
		backupConfigs, _ = strconv.ParseBool(configs[0])
		if backupConfigs {
			fullCommand = fmt.Sprintf("%s --configs", fullCommand)
		}
	}
	if namedCollections, exist := api.getQueryParameter(query, "named-collections"); exist {
		backupNamedCollections, _ = strconv.ParseBool(namedCollections)
		if backupNamedCollections {
			fullCommand = fmt.Sprintf("%s --named-collections", fullCommand)
		}
	}
	if _, exist := api.getQueryParameter(query, "skip_check_parts_columns"); exist {
		skipCheckPartsColumns = true
		fullCommand = fmt.Sprintf("%s --skip-check-parts-columns", fullCommand)
	}
	if _, exist := api.getQueryParameter(query, "delete_source"); exist {
		deleteSource = true
		fullCommand = fmt.Sprintf("%s --delete-source", fullCommand)
	}
	if skipProjectionsFromQuery, exist := api.getQueryParameter(query, "skip_projections"); exist {
		skipProjections = append(skipProjections, skipProjectionsFromQuery)
		fullCommand = fmt.Sprintf("%s --skip-projections=%s", fullCommand, skipProjectionsFromQuery)
	}

	if status.Current.CheckCommandInProgress(fullCommand) {
		log.Warn().Msgf("%s error: %v", fullCommand, ErrAPILocked)
		api.writeError(w, http.StatusLocked, "watch", ErrAPILocked)
		return
	}

	commandId, _ := status.Current.Start(fullCommand)
	go func() {
		b := backup.NewBackuper(cfg)
		err := b.Watch(watchInterval, fullInterval, watchBackupNameTemplate, tablePattern, partitionsToBackup, skipProjections, schemaOnly, backupRBAC, backupConfigs, backupNamedCollections, skipCheckPartsColumns, deleteSource, api.clickhouseBackupVersion, commandId, api.GetMetrics(), api.cliCtx)
		api.handleWatchResponse(commandId, err)
	}()
	api.sendJSONEachRow(w, http.StatusCreated, struct {
		Status    string `json:"status"`
		Operation string `json:"operation"`
		Command   string `json:"command"`
	}{
		Status:    "acknowledged",
		Operation: "watch",
		Command:   fullCommand,
	})
}

// httpCleanHandler - clean ./shadow directory
func (api *APIServer) httpCleanHandler(w http.ResponseWriter, _ *http.Request) {
	var err error
	fullCommand := "clean"
	commandId, ctx := status.Current.Start(fullCommand)
	b := backup.NewBackuper(api.config)
	err = b.Clean(ctx)
	defer status.Current.Stop(commandId, err)
	if err != nil {
		log.Error().Msgf("Clean error: %v", err)
		api.writeError(w, http.StatusInternalServerError, "clean", err)
		return
	}
	api.sendJSONEachRow(w, http.StatusOK, struct {
		Status    string `json:"status"`
		Operation string `json:"operation"`
	}{
		Status:    "success",
		Operation: "clean",
	})
}

// httpCleanRemoteBrokenHandler - delete all remote backups with `broken` in description
func (api *APIServer) httpCleanLocalBrokenHandler(w http.ResponseWriter, _ *http.Request) {
	cfg, err := api.ReloadConfig(w, "clean_local_broken")
	if err != nil {
		return
	}
	commandId, _ := status.Current.Start("clean_local_broken")
	defer status.Current.Stop(commandId, err)

	b := backup.NewBackuper(cfg)
	err = b.CleanLocalBroken(commandId)
	if err != nil {
		log.Error().Msgf("Clean local broken error: %v", err)
		api.writeError(w, http.StatusInternalServerError, "clean_local_broken", err)
		return
	}
	go func() {
		if metricsErr := api.UpdateBackupMetrics(context.Background(), true); metricsErr != nil {
			log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
		}
	}()

	api.sendJSONEachRow(w, http.StatusOK, struct {
		Status    string `json:"status"`
		Operation string `json:"operation"`
	}{
		Status:    "success",
		Operation: "clean_local_broken",
	})
}

func (api *APIServer) httpCleanRemoteBrokenHandler(w http.ResponseWriter, _ *http.Request) {
	cfg, err := api.ReloadConfig(w, "clean_remote_broken")
	if err != nil {
		return
	}
	commandId, _ := status.Current.Start("clean_remote_broken")
	defer status.Current.Stop(commandId, err)

	b := backup.NewBackuper(cfg)
	err = b.CleanRemoteBroken(commandId)
	if err != nil {
		log.Error().Msgf("Clean remote broken error: %v", err)
		api.writeError(w, http.StatusInternalServerError, "clean_remote_broken", err)
		return
	}
	go func() {
		if metricsErr := api.UpdateBackupMetrics(context.Background(), false); metricsErr != nil {
			log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
		}
	}()

	api.sendJSONEachRow(w, http.StatusOK, struct {
		Status    string `json:"status"`
		Operation string `json:"operation"`
	}{
		Status:    "success",
		Operation: "clean_remote_broken",
	})
}

// httpUploadHandler - upload a backup to remote storage
func (api *APIServer) httpUploadHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		api.writeError(w, http.StatusLocked, "upload", ErrAPILocked)
		return
	}
	cfg, err := api.ReloadConfig(w, "upload")
	if err != nil {
		return
	}
	vars := mux.Vars(r)
	query := r.URL.Query()
	deleteSource := false
	diffFrom := ""
	diffFromRemote := ""
	name := utils.CleanBackupNameRE.ReplaceAllString(vars["name"], "")
	tablePattern := ""
	partitionsToBackup := make([]string, 0)
	skipProjections := make([]string, 0)
	schemaOnly := false
	rbacOnly := false
	configsOnly := false
	namedCollectionsOnly := false
	resume := false
	fullCommand := "upload"
	operationId, _ := uuid.NewUUID()

	if _, exist := api.getQueryParameter(query, "delete-source"); exist {
		deleteSource = true
		fullCommand = fmt.Sprintf("%s --deleteSource", fullCommand)
	}

	if df, exist := api.getQueryParameter(query, "diff-from"); exist {
		diffFrom = df
		fullCommand = fmt.Sprintf("%s --diff-from=\"%s\"", fullCommand, diffFrom)
	}
	if df, exist := api.getQueryParameter(query, "diff-from-remote"); exist {
		diffFromRemote = df
		fullCommand = fmt.Sprintf("%s --diff-from-remote=\"%s\"", fullCommand, diffFromRemote)
	}
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if partitions, exist := query["partitions"]; exist {
		partitionsToBackup = append(partitionsToBackup, partitions...)
		fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, strings.Join(partitions, "\" --partitions=\""))
	}
	if _, exist := query["schema"]; exist {
		schemaOnly = true
		fullCommand += " --schema"
	}
	if _, exist := query["rbac-only"]; exist {
		rbacOnly = true
		fullCommand += " --rbac-only"
	}
	if _, exist := query["configs-only"]; exist {
		configsOnly = true
		fullCommand += " --configs-only"
	}
	if _, exist := api.getQueryParameter(query, "named-collections-only"); exist {
		namedCollectionsOnly = true
		fullCommand += " --named-collections-only"
	}
	if skipProjectionsFromQuery, exist := query["skip-projections"]; exist {
		skipProjections = skipProjectionsFromQuery
		fullCommand += " --skip-projections=" + strings.Join(skipProjectionsFromQuery, ",")
	}
	if _, exist := query["resume"]; exist {
		resume = true
	}
	if _, exist := query["resumable"]; exist {
		resume = true
	}
	if resume {
		fullCommand += " --resume"
	}

	fullCommand = fmt.Sprint(fullCommand, " ", name)

	callback, err := parseCallback(query)
	if err != nil {
		log.Error().Err(err).Send()
		api.writeError(w, http.StatusBadRequest, "upload", err)
		return
	}

	go func() {
		commandId, _ := status.Current.StartWithOperationId(fullCommand, operationId.String())
		err, _ := api.metrics.ExecuteWithMetrics("upload", 0, func() error {
			b := backup.NewBackuper(cfg)
			return b.Upload(name, deleteSource, diffFrom, diffFromRemote, tablePattern, partitionsToBackup, skipProjections, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, resume, api.cliApp.Version, commandId)
		})
		if err != nil {
			log.Error().Msgf("Upload error: %v", err)
			status.Current.Stop(commandId, err)
			api.errorCallback(context.Background(), err, operationId.String(), callback)
			return
		}
		go func() {
			if metricsErr := api.UpdateBackupMetrics(context.Background(), false); metricsErr != nil {
				log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
			}
		}()
		status.Current.Stop(commandId, nil)
		api.successCallback(context.Background(), operationId.String(), callback)
	}()
	api.sendJSONEachRow(w, http.StatusOK, struct {
		Status      string `json:"status"`
		Operation   string `json:"operation"`
		BackupName  string `json:"backup_name"`
		BackupFrom  string `json:"backup_from,omitempty"`
		Diff        bool   `json:"diff"`
		OperationId string `json:"operation_id"`
	}{
		Status:      "acknowledged",
		Operation:   "upload",
		BackupName:  name,
		BackupFrom:  diffFrom,
		Diff:        diffFrom != "",
		OperationId: operationId.String(),
	})
}

var databaseMappingRE = regexp.MustCompile(`[\w+]:[\w+]`)
var tableMappingRE = regexp.MustCompile(`[\w+]:[\w+]`)

// httpRestoreHandler - restore a backup from local storage
func (api *APIServer) httpRestoreHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		api.writeError(w, http.StatusLocked, "restore", ErrAPILocked)
		return
	}
	_, err := api.ReloadConfig(w, "restore")
	if err != nil {
		return
	}
	vars := mux.Vars(r)
	tablePattern := ""
	databaseMappingToRestore := make([]string, 0)
	tableMappingToRestore := make([]string, 0)
	partitionsToBackup := make([]string, 0)
	schemaOnly := false
	dataOnly := false
	dropExists := false
	ignoreDependencies := false
	restoreRBAC := false
	rbacOnly := false
	restoreConfigs := false
	configsOnly := false
	restoreNamedCollections := false
	namedCollectionsOnly := false
	skipProjections := make([]string, 0)
	resume := false
	restoreSchemaAsAttach := false
	replicatedCopyToDetached := false
	fullCommand := "restore"
	operationId, _ := uuid.NewUUID()

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	databaseMappingQueryParamName := "restore_database_mapping"
	databaseMappingQueryParamNames := []string{
		strings.Replace(databaseMappingQueryParamName, "_", "-", -1),
		strings.Replace(databaseMappingQueryParamName, "-", "_", -1),
	}
	for _, queryParamName := range databaseMappingQueryParamNames {
		if databaseMappingQuery, exist := query[queryParamName]; exist {
			for _, databaseMapping := range databaseMappingQuery {
				mappingItems := strings.Split(databaseMapping, ",")
				for _, m := range mappingItems {
					if strings.Count(m, ":") != 1 || !databaseMappingRE.MatchString(m) {
						api.writeError(w, http.StatusInternalServerError, "restore", fmt.Errorf("invalid values in restore_database_mapping %s", m))
						return

					}
				}
				databaseMappingToRestore = append(databaseMappingToRestore, mappingItems...)
			}

			fullCommand = fmt.Sprintf("%s --restore-database-mapping=\"%s\"", fullCommand, strings.Join(databaseMappingToRestore, ","))
		}
	}

	// https://github.com/Altinity/clickhouse-backup/issues/937
	tableMappingQueryParamName := "restore_table_mapping"
	tableMappingQueryParamNames := []string{
		strings.Replace(tableMappingQueryParamName, "_", "-", -1),
		strings.Replace(tableMappingQueryParamName, "-", "_", -1),
	}
	for _, queryParamName := range tableMappingQueryParamNames {
		if tableMappingQuery, exist := query[queryParamName]; exist {
			for _, tableMapping := range tableMappingQuery {
				mappingItems := strings.Split(tableMapping, ",")
				for _, m := range mappingItems {
					if strings.Count(m, ":") != 1 || !tableMappingRE.MatchString(m) {
						api.writeError(w, http.StatusInternalServerError, "restore", fmt.Errorf("invalid values in restore_table_mapping %s", m))
						return
					}
				}
				tableMappingToRestore = append(tableMappingToRestore, mappingItems...)
			}

			fullCommand = fmt.Sprintf("%s --restore-table-mapping=\"%s\"", fullCommand, strings.Join(tableMappingToRestore, ","))
		}
	}

	if partitions, exist := query["partitions"]; exist {
		partitionsToBackup = append(partitionsToBackup, partitions...)
		fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, strings.Join(partitions, "\" --partitions=\""))
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
		dropExists = true
		fullCommand += " --drop"
	}
	if _, exist := query["rm"]; exist {
		dropExists = true
		fullCommand += " --rm"
	}
	if _, exists := api.getQueryParameter(query, "ignore_dependencies"); exists {
		ignoreDependencies = true
		fullCommand += " --ignore-dependencies"
	}
	if _, exist := query["rbac"]; exist {
		restoreRBAC = true
		fullCommand += " --rbac"
	}
	if _, exist := api.getQueryParameter(query, "rbac-only"); exist {
		rbacOnly = true
		fullCommand += " --rbac-only"
	}
	if _, exist := query["configs"]; exist {
		restoreConfigs = true
		fullCommand += " --configs"
	}
	if _, exist := api.getQueryParameter(query, "configs-only"); exist {
		configsOnly = true
		fullCommand += " --configs-only"
	}
	if _, exist := api.getQueryParameter(query, "named-collections"); exist {
		restoreNamedCollections = true
		fullCommand += " --named-collections"
	}
	if _, exist := api.getQueryParameter(query, "named-collections-only"); exist {
		namedCollectionsOnly = true
		fullCommand += " --named-collections-only"
	}
	if skipProjectionsFromQuery, exist := api.getQueryParameter(query, "skip-projections"); exist {
		skipProjections = append(skipProjections, skipProjectionsFromQuery)
		fullCommand += " --skip-projections=" + strings.Join(skipProjections, ",")
	}
	if _, exist := query["resumable"]; exist {
		resume = true
		fullCommand += " --resumable"
	}
	if _, exist := query["resume"]; exist {
		resume = true
		fullCommand += " --resume"
	}

	// https://github.com/Altinity/clickhouse-backup/issues/868
	restoreSchemAsAttachParamName := "restore_schema_as_attach"
	restoreSchemAsAttachParamNames := []string{
		strings.Replace(restoreSchemAsAttachParamName, "_", "-", -1),
		strings.Replace(restoreSchemAsAttachParamName, "-", "_", -1),
	}
	for _, paramName := range restoreSchemAsAttachParamNames {
		if _, exist := api.getQueryParameter(query, paramName); exist {
			restoreSchemaAsAttach = true
			fullCommand += " --restore-schema-as-attach"
		}
	}

	// Handle replicated-copy-to-detached parameter
	replicatedCopyToDetachedParamName := "replicated_copy_to_detached"
	replicatedCopyToDetachedParamNames := []string{
		strings.Replace(replicatedCopyToDetachedParamName, "_", "-", -1),
		strings.Replace(replicatedCopyToDetachedParamName, "-", "_", -1),
	}
	for _, paramName := range replicatedCopyToDetachedParamNames {
		if _, exist := api.getQueryParameter(query, paramName); exist {
			replicatedCopyToDetached = true
			fullCommand += " --replicated-copy-to-detached"
		}
	}

	name := utils.CleanBackupNameRE.ReplaceAllString(vars["name"], "")
	fullCommand += fmt.Sprintf(" %s", name)

	callback, err := parseCallback(query)
	if err != nil {
		log.Error().Err(err).Send()
		api.writeError(w, http.StatusBadRequest, "restore", err)
		return
	}

	commandId, _ := status.Current.StartWithOperationId(fullCommand, operationId.String())
	go func() {
		err, _ := api.metrics.ExecuteWithMetrics("restore", 0, func() error {
			b := backup.NewBackuper(api.config)
			return b.Restore(name, tablePattern, databaseMappingToRestore, tableMappingToRestore, partitionsToBackup, skipProjections, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, restoreSchemaAsAttach, replicatedCopyToDetached, api.cliApp.Version, commandId)
		})
		go func() {
			if metricsErr := api.UpdateBackupMetrics(context.Background(), true); metricsErr != nil {
				log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
			}
		}()
		status.Current.Stop(commandId, err)
		if err != nil {
			log.Error().Msgf("API /backup/restore error: %v", err)
			api.errorCallback(context.Background(), err, operationId.String(), callback)
			return
		}
		api.successCallback(context.Background(), operationId.String(), callback)
	}()
	api.sendJSONEachRow(w, http.StatusOK, struct {
		Status      string `json:"status"`
		Operation   string `json:"operation"`
		BackupName  string `json:"backup_name"`
		OperationId string `json:"operation_id"`
	}{
		Status:      "acknowledged",
		Operation:   "restore",
		BackupName:  name,
		OperationId: operationId.String(),
	})
}

// httpRestoreRemoteHandler - download and restore a backup from remote storage
func (api *APIServer) httpRestoreRemoteHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		api.writeError(w, http.StatusLocked, "restore_remote", ErrAPILocked)
		return
	}
	cfg, err := api.ReloadConfig(w, "restore_remote")
	if err != nil {
		return
	}
	vars := mux.Vars(r)
	tablePattern := ""
	databaseMappingToRestore := make([]string, 0)
	tableMappingToRestore := make([]string, 0)
	partitionsToBackup := make([]string, 0)
	schemaOnly := false
	dataOnly := false
	dropExists := false
	ignoreDependencies := false
	restoreRBAC := false
	rbacOnly := false
	restoreConfigs := false
	configsOnly := false
	restoreNamedCollections := false
	namedCollectionsOnly := false
	skipProjections := make([]string, 0)
	resume := false
	restoreSchemaAsAttach := false
	replicatedCopyToDetached := false
	hardlinkExistsFiles := false
	fullCommand := "restore_remote"
	operationId, _ := uuid.NewUUID()

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	databaseMappingQueryParamName := "restore_database_mapping"
	databaseMappingQueryParamNames := []string{
		strings.Replace(databaseMappingQueryParamName, "_", "-", -1),
		strings.Replace(databaseMappingQueryParamName, "-", "_", -1),
	}
	for _, queryParamName := range databaseMappingQueryParamNames {
		if databaseMappingQuery, exist := query[queryParamName]; exist {
			for _, databaseMapping := range databaseMappingQuery {
				mappingItems := strings.Split(databaseMapping, ",")
				for _, m := range mappingItems {
					if strings.Count(m, ":") != 1 || !databaseMappingRE.MatchString(m) {
						api.writeError(w, http.StatusInternalServerError, "restore_remote", fmt.Errorf("invalid values in restore_database_mapping %s", m))
						return

					}
				}
				databaseMappingToRestore = append(databaseMappingToRestore, mappingItems...)
			}

			fullCommand = fmt.Sprintf("%s --restore-database-mapping=\"%s\"", fullCommand, strings.Join(databaseMappingToRestore, ","))
		}
	}

	// https://github.com/Altinity/clickhouse-backup/issues/937
	tableMappingQueryParamName := "restore_table_mapping"
	tableMappingQueryParamNames := []string{
		strings.Replace(tableMappingQueryParamName, "_", "-", -1),
		strings.Replace(tableMappingQueryParamName, "-", "_", -1),
	}
	for _, queryParamName := range tableMappingQueryParamNames {
		if tableMappingQuery, exist := query[queryParamName]; exist {
			for _, tableMapping := range tableMappingQuery {
				mappingItems := strings.Split(tableMapping, ",")
				for _, m := range mappingItems {
					if strings.Count(m, ":") != 1 || !tableMappingRE.MatchString(m) {
						api.writeError(w, http.StatusInternalServerError, "restore_remote", fmt.Errorf("invalid values in restore_table_mapping %s", m))
						return
					}
				}
				tableMappingToRestore = append(tableMappingToRestore, mappingItems...)
			}

			fullCommand = fmt.Sprintf("%s --restore-table-mapping=\"%s\"", fullCommand, strings.Join(tableMappingToRestore, ","))
		}
	}

	if partitions, exist := query["partitions"]; exist {
		partitionsToBackup = append(partitionsToBackup, partitions...)
		fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, strings.Join(partitions, "\" --partitions=\""))
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
		dropExists = true
		fullCommand += " --drop"
	}
	if _, exist := query["rm"]; exist {
		dropExists = true
		fullCommand += " --rm"
	}
	if _, exists := api.getQueryParameter(query, "ignore_dependencies"); exists {
		ignoreDependencies = true
		fullCommand += " --ignore-dependencies"
	}
	if _, exist := query["rbac"]; exist {
		restoreRBAC = true
		fullCommand += " --rbac"
	}
	if _, exist := api.getQueryParameter(query, "rbac-only"); exist {
		rbacOnly = true
		fullCommand += " --rbac-only"
	}
	if _, exist := query["configs"]; exist {
		restoreConfigs = true
		fullCommand += " --configs"
	}
	if _, exist := api.getQueryParameter(query, "configs-only"); exist {
		configsOnly = true
		fullCommand += " --configs-only"
	}
	if _, exist := api.getQueryParameter(query, "named-collections"); exist {
		restoreNamedCollections = true
		fullCommand += " --named-collections"
	}
	if _, exist := api.getQueryParameter(query, "named-collections-only"); exist {
		namedCollectionsOnly = true
		fullCommand += " --named-collections-only"
	}
	if skipProjectionsFromQuery, exist := api.getQueryParameter(query, "skip-projections"); exist {
		skipProjections = append(skipProjections, skipProjectionsFromQuery)
		fullCommand += " --skip-projections=" + strings.Join(skipProjections, ",")
	}
	if _, exist := query["resumable"]; exist {
		resume = true
		fullCommand += " --resumable"
	}
	if _, exist := query["resume"]; exist {
		resume = true
		fullCommand += " --resume"
	}

	// https://github.com/Altinity/clickhouse-backup/issues/868
	restoreSchemAsAttachParamName := "restore_schema_as_attach"
	restoreSchemAsAttachParamNames := []string{
		strings.Replace(restoreSchemAsAttachParamName, "_", "-", -1),
		strings.Replace(restoreSchemAsAttachParamName, "-", "_", -1),
	}
	for _, paramName := range restoreSchemAsAttachParamNames {
		if _, exist := api.getQueryParameter(query, paramName); exist {
			restoreSchemaAsAttach = true
			fullCommand += " --restore-schema-as-attach"
		}
	}

	// Handle replicated-copy-to-detached parameter
	replicatedCopyToDetachedParamName := "replicated_copy_to_detached"
	replicatedCopyToDetachedParamNames := []string{
		strings.Replace(replicatedCopyToDetachedParamName, "_", "-", -1),
		strings.Replace(replicatedCopyToDetachedParamName, "-", "_", -1),
	}
	for _, paramName := range replicatedCopyToDetachedParamNames {
		if _, exist := api.getQueryParameter(query, paramName); exist {
			replicatedCopyToDetached = true
			fullCommand += " --replicated-copy-to-detached"
		}
	}

	if _, exist := api.getQueryParameter(query, "hardlink_exists_files"); exist {
		hardlinkExistsFiles = true
		fullCommand += " --hardlink-exists-files"
	}

	name := utils.CleanBackupNameRE.ReplaceAllString(vars["name"], "")
	fullCommand += fmt.Sprintf(" %s", name)

	callback, err := parseCallback(query)
	if err != nil {
		log.Error().Err(err).Send()
		api.writeError(w, http.StatusBadRequest, "restore_remote", err)
		return
	}

	commandId, _ := status.Current.Start(fullCommand)
	go func() {
		err, _ := api.metrics.ExecuteWithMetrics("restore_remote", 0, func() error {
			b := backup.NewBackuper(cfg)
			return b.RestoreFromRemote(name, tablePattern, databaseMappingToRestore, tableMappingToRestore, partitionsToBackup, skipProjections, schemaOnly, dataOnly, dropExists, ignoreDependencies, restoreRBAC, rbacOnly, restoreConfigs, configsOnly, restoreNamedCollections, namedCollectionsOnly, resume, restoreSchemaAsAttach, replicatedCopyToDetached, hardlinkExistsFiles, false, api.cliApp.Version, commandId)
		})
		go func() {
			if metricsErr := api.UpdateBackupMetrics(context.Background(), true); metricsErr != nil {
				log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
			}
		}()
		status.Current.Stop(commandId, err)
		if err != nil {
			log.Error().Msgf("API /backup/restore_remote error: %v", err)
			api.errorCallback(context.Background(), err, operationId.String(), callback)
			return
		}
		api.successCallback(context.Background(), operationId.String(), callback)
	}()
	api.sendJSONEachRow(w, http.StatusOK, struct {
		Status      string `json:"status"`
		Operation   string `json:"operation"`
		BackupName  string `json:"backup_name"`
		OperationId string `json:"operation_id"`
	}{
		Status:      "acknowledged",
		Operation:   "restore_remote",
		BackupName:  name,
		OperationId: operationId.String(),
	})
}

// httpDownloadHandler - download a backup from remote to local storage
func (api *APIServer) httpDownloadHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		api.writeError(w, http.StatusLocked, "download", ErrAPILocked)
		return
	}
	cfg, err := api.ReloadConfig(w, "download")
	if err != nil {
		return
	}
	vars := mux.Vars(r)
	name := strings.ReplaceAll(vars["name"], "/", "")
	name = strings.ReplaceAll(name, "/", "")
	query := r.URL.Query()
	tablePattern := ""
	partitionsToBackup := make([]string, 0)
	schemaOnly := false
	rbacOnly := false
	configsOnly := false
	namedCollectionsOnly := false
	resume := false
	hardlinkExistsFiles := false
	fullCommand := "download"
	operationId, _ := uuid.NewUUID()

	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
		fullCommand = fmt.Sprintf("%s --tables=\"%s\"", fullCommand, tablePattern)
	}
	if partitions, exist := query["partitions"]; exist {
		partitionsToBackup = append(partitionsToBackup, partitions...)
		fullCommand = fmt.Sprintf("%s --partitions=\"%s\"", fullCommand, strings.Join(partitions, "\" --partitions=\""))
	}
	if _, exist := query["schema"]; exist {
		schemaOnly = true
		fullCommand += " --schema"
	}
	if _, exist := query["rbac-only"]; exist {
		rbacOnly = true
		fullCommand += " --rbac-only"
	}
	if _, exist := query["configs-only"]; exist {
		configsOnly = true
		fullCommand += " --configs-only"
	}
	if _, exist := api.getQueryParameter(query, "named-collections-only"); exist {
		namedCollectionsOnly = true
		fullCommand += " --named-collections-only"
	}
	if _, exist := query["resumable"]; exist {
		resume = true
		fullCommand += " --resumable"
	}
	if _, exist := query["resume"]; exist {
		resume = true
		fullCommand += " --resume"
	}

	if _, exist := api.getQueryParameter(query, "hardlink_exists_files"); exist {
		hardlinkExistsFiles = true
		fullCommand += " --hardlink-exists-files"
	}

	fullCommand += fmt.Sprintf(" %s", name)

	callback, err := parseCallback(query)
	if err != nil {
		log.Error().Err(err).Send()
		api.writeError(w, http.StatusBadRequest, "download", err)
		return
	}

	go func() {
		commandId, _ := status.Current.StartWithOperationId(fullCommand, operationId.String())
		err, _ := api.metrics.ExecuteWithMetrics("download", 0, func() error {
			b := backup.NewBackuper(cfg)
			return b.Download(name, tablePattern, partitionsToBackup, schemaOnly, rbacOnly, configsOnly, namedCollectionsOnly, resume, hardlinkExistsFiles, api.cliApp.Version, commandId)
		})
		if err != nil {
			log.Error().Msgf("API /backup/download error: %v", err)
			status.Current.Stop(commandId, err)
			api.errorCallback(context.Background(), err, operationId.String(), callback)
			return
		}
		go func() {
			if metricsErr := api.UpdateBackupMetrics(context.Background(), true); metricsErr != nil {
				log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
			}
		}()
		status.Current.Stop(commandId, nil)
		api.successCallback(context.Background(), operationId.String(), callback)
	}()
	api.sendJSONEachRow(w, http.StatusOK, struct {
		Status      string `json:"status"`
		Operation   string `json:"operation"`
		BackupName  string `json:"backup_name"`
		OperationId string `json:"operation_id"`
	}{
		Status:      "acknowledged",
		Operation:   "download",
		BackupName:  name,
		OperationId: operationId.String(),
	})
}

// httpDeleteHandler - delete a backup from local or remote storage
func (api *APIServer) httpDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if !api.config.API.AllowParallel && status.Current.InProgress() {
		log.Warn().Err(ErrAPILocked).Send()
		api.writeError(w, http.StatusLocked, "delete", ErrAPILocked)
		return
	}
	cfg, err := api.ReloadConfig(w, "delete")
	if err != nil {
		return
	}
	vars := mux.Vars(r)
	fullCommand := fmt.Sprintf("delete %s %s", vars["where"], vars["name"])
	commandId, ctx := status.Current.Start(fullCommand)
	b := backup.NewBackuper(cfg)
	switch vars["where"] {
	case "local":
		err = b.RemoveBackupLocal(ctx, vars["name"], nil)
	case "remote":
		err = b.RemoveBackupRemote(ctx, vars["name"])
	default:
		err = fmt.Errorf("backup location must be 'local' or 'remote'")
	}
	status.Current.Stop(commandId, err)
	if err != nil {
		log.Error().Msgf("delete backup error: %v", err)
		api.writeError(w, http.StatusInternalServerError, "delete", err)
		return
	}
	go func() {
		if metricsErr := api.UpdateBackupMetrics(context.Background(), vars["where"] == "local"); metricsErr != nil {
			log.Error().Msgf("UpdateBackupMetrics return error: %v", metricsErr)
		}
	}()
	api.sendJSONEachRow(w, http.StatusOK, struct {
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

func (api *APIServer) httpStatusHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if operationId := query.Get("operationid"); operationId != "" {
		api.sendJSONEachRow(w, http.StatusOK, status.Current.GetStatusByOperationId(operationId))
		return
	}

	api.sendJSONEachRow(w, http.StatusOK, status.Current.GetStatus(true, "", 0))
}

func (api *APIServer) UpdateBackupMetrics(ctx context.Context, onlyLocal bool) error {
	// calc lastXXX metrics, fix https://github.com/Altinity/clickhouse-backup/issues/515
	var lastBackupCreateLocal *time.Time
	startTime := time.Now()
	lastSizeLocal := uint64(0)
	numberBackupsLocal := 0
	localDataSize := float64(0)

	log.Info().Msgf("Update backup metrics start (onlyLocal=%v)", onlyLocal)
	if !api.config.API.EnableMetrics {
		return nil
	}
	b := backup.NewBackuper(api.config)
	localBackups, _, err := b.GetLocalBackups(ctx, nil)
	if err != nil {
		return err
	}
	if len(localBackups) > 0 {
		numberBackupsLocal = len(localBackups)
		lastBackup := localBackups[numberBackupsLocal-1]
		lastSizeLocal = lastBackup.DataSize + lastBackup.MetadataSize + lastBackup.ConfigSize + lastBackup.RBACSize
		lastBackupCreateLocal = &lastBackup.CreationDate
		api.metrics.LastBackupSizeLocal.Set(float64(lastSizeLocal))
		api.metrics.NumberBackupsLocal.Set(float64(numberBackupsLocal))
	} else {
		api.metrics.LastBackupSizeLocal.Set(0)
		api.metrics.NumberBackupsLocal.Set(0)
	}
	if localDataSize, err = b.GetLocalDataSize(ctx); err != nil {
		return err
	}
	if localDataSize > 0 {
		api.metrics.LocalDataSize.Set(localDataSize)
	}
	if api.config.General.RemoteStorage == "none" || onlyLocal {
		log.Info().Fields(map[string]interface{}{
			"duration":              utils.HumanizeDuration(time.Since(startTime)),
			"LastBackupCreateLocal": lastBackupCreateLocal,
			"LastBackupSizeLocal":   lastSizeLocal,
			"NumberBackupsLocal":    numberBackupsLocal,
			"LocalDataSize":         utils.FormatBytes(uint64(localDataSize)),
		}).Msg("Update backup metrics finish")
		return nil
	}
	//onlyLocal false
	var lastBackupCreateRemote *time.Time
	var lastBackupUpload *time.Time
	lastSizeRemote := uint64(0)
	numberBackupsRemote := 0
	numberBackupsRemoteBroken := 0

	remoteBackups, err := b.GetRemoteBackups(ctx, false)
	if err != nil {
		return err
	}
	if len(remoteBackups) > 0 {
		numberBackupsRemote = len(remoteBackups)
		for _, b := range remoteBackups {
			if b.Broken != "" {
				numberBackupsRemoteBroken++
			}
		}
		lastBackup := remoteBackups[numberBackupsRemote-1]
		lastSizeRemote = lastBackup.GetFullSize()
		lastBackupCreateRemote = &lastBackup.CreationDate
		lastBackupUpload = &lastBackup.UploadDate
		api.metrics.LastBackupSizeRemote.Set(float64(lastSizeRemote))
		api.metrics.NumberBackupsRemote.Set(float64(numberBackupsRemote))
		api.metrics.NumberBackupsRemoteBroken.Set(float64(numberBackupsRemoteBroken))
	} else {
		api.metrics.LastBackupSizeRemote.Set(0)
		api.metrics.NumberBackupsRemote.Set(0)
		api.metrics.NumberBackupsRemoteBroken.Set(0)
	}

	if lastBackupCreateLocal != nil {
		api.metrics.LastFinish["create"].Set(float64(lastBackupCreateLocal.Unix()))
	}
	if lastBackupCreateRemote != nil {
		api.metrics.LastFinish["create_remote"].Set(float64(lastBackupCreateRemote.Unix()))
		if lastBackupCreateLocal == nil || lastBackupCreateRemote.Unix() > lastBackupCreateLocal.Unix() {
			api.metrics.LastFinish["create"].Set(float64(lastBackupCreateRemote.Unix()))
		}
	}
	if lastBackupUpload != nil {
		if lastBackupCreateRemote == nil || lastBackupUpload.Unix() > lastBackupCreateRemote.Unix() {
			api.metrics.LastFinish["create_remote"].Set(float64(lastBackupUpload.Unix()))
		} else {
			api.metrics.LastFinish["upload"].Set(float64(lastBackupCreateRemote.Unix()))
		}
	}

	log.Info().Fields(map[string]interface{}{
		"duration":               utils.HumanizeDuration(time.Since(startTime)),
		"LastBackupCreateLocal":  lastBackupCreateLocal,
		"LastBackupCreateRemote": lastBackupCreateRemote,
		"LastBackupUpload":       lastBackupUpload,
		"LastBackupSizeRemote":   lastSizeRemote,
		"LastBackupSizeLocal":    lastSizeLocal,
		"NumberBackupsLocal":     numberBackupsLocal,
		"NumberBackupsRemote":    numberBackupsRemote,
		"LocalDataSize":          utils.FormatBytes(uint64(localDataSize)),
	}).Msg("Update backup metrics finish")

	return nil
}

func (api *APIServer) registerMetricsHandlers(r *mux.Router, enableMetrics bool, enablePprof bool) {
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		api.sendJSONEachRow(w, http.StatusOK, struct {
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

func (api *APIServer) CreateIntegrationTables() error {
	log.Info().Msgf("Create integration tables")
	ch := &clickhouse.ClickHouse{
		Config: &api.config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %w", err)
	}
	defer ch.Close()
	port := "80"
	if strings.Contains(api.config.API.ListenAddr, ":") {
		port = api.config.API.ListenAddr[strings.Index(api.config.API.ListenAddr, ":")+1:]
	}
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
	version, err := ch.GetVersion(context.Background())
	if err != nil {
		return err
	}
	if version >= 21001000 {
		settings = "SETTINGS input_format_skip_unknown_fields=1"
	}
	disks, err := ch.GetDisks(context.Background(), true)
	if err != nil {
		return err
	}
	defaultDataPath, err := ch.GetDefaultPath(disks)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("CREATE TABLE system.backup_actions (command String, start DateTime, finish DateTime, status String, error String, operation_id String) ENGINE=URL('%s://%s:%s/backup/actions%s', JSONEachRow) %s", schema, host, port, auth, settings)
	if err := ch.CreateTable(clickhouse.Table{Database: "system", Name: "backup_actions"}, query, true, false, "", 0, defaultDataPath, false, ""); err != nil {
		return err
	}
	query = fmt.Sprintf("CREATE TABLE system.backup_list (name String, created DateTime, size UInt64, data_size UInt64, object_disk_size UInt64,metadata_size UInt64,rbac_size UInt64,config_size UInt64, named_collection_size UInt64, compressed_size UInt64, location String, required String, desc String) ENGINE=URL('%s://%s:%s/backup/list%s', JSONEachRow) %s", schema, host, port, auth, settings)
	if err := ch.CreateTable(clickhouse.Table{Database: "system", Name: "backup_list"}, query, true, false, "", 0, defaultDataPath, false, ""); err != nil {
		return err
	}
	query = fmt.Sprintf("CREATE TABLE system.backup_version (version String) ENGINE=URL('%s://%s:%s/backup/version%s', JSONEachRow) %s", schema, host, port, auth, settings)
	if err := ch.CreateTable(clickhouse.Table{Database: "system", Name: "backup_version"}, query, true, false, "", 0, defaultDataPath, false, ""); err != nil {
		return err
	}
	return nil
}

func (api *APIServer) ReloadConfig(w http.ResponseWriter, command string) (*config.Config, error) {
	cfg, err := config.LoadConfig(api.configPath)
	if err != nil {
		log.Error().Msgf("config.LoadConfig(%s) return error: %v", api.configPath, err)
		if w != nil {
			api.writeError(w, http.StatusInternalServerError, command, err)
		}
		return nil, err
	}
	api.config = cfg
	api.metrics.NumberBackupsRemoteExpected.Set(float64(cfg.General.BackupsToKeepRemote))
	api.metrics.NumberBackupsLocalExpected.Set(float64(cfg.General.BackupsToKeepLocal))
	return cfg, nil
}

func (api *APIServer) ResumeOperationsAfterRestart() error {
	ch := clickhouse.ClickHouse{
		Config: &api.config.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return err
	}
	defer func() {
		if err := ch.GetConn().Close(); err != nil {
			log.Error().Msgf("ResumeOperationsAfterRestart can't close clickhouse connection: %v", err)
		}
	}()
	disks, err := ch.GetDisks(context.Background(), true)
	if err != nil {
		return err
	}
	defaultDiskPath, err := ch.GetDefaultPath(disks)
	if err != nil {
		return err
	}
	embeddedBackupDiskPath, err := ch.GetEmbeddedBackupPath(disks)
	if err != nil {
		return err
	}
	backupList, err := os.ReadDir(path.Join(defaultDiskPath, "backup"))
	if err != nil {
		return err
	}
	for _, backupItem := range backupList {
		if backupItem.IsDir() {
			backupName := backupItem.Name()
			stateFiles, err := filepath.Glob(path.Join(defaultDiskPath, "backup", backupName, "*.state2"))
			if err != nil {
				return err
			}
			embeddedStateFiles, err := filepath.Glob(path.Join(embeddedBackupDiskPath, "backup", backupName, "*.state2"))
			if err != nil {
				return err
			}
			stateFiles = append(stateFiles, embeddedStateFiles...)
			for _, stateFile := range stateFiles {
				command := strings.TrimSuffix(filepath.Base(stateFile), ".state2")
				state := resumable.NewState(strings.TrimSuffix(filepath.Dir(stateFile), filepath.Join("backup", backupName)), backupName, command, nil)
				params := state.GetParams()
				state.Close()
				if !api.config.API.AllowParallel && status.Current.InProgress() {
					return fmt.Errorf("another commands in progress")
				}
				switch command {
				case "download":
				case "upload":
					args := make([]string, 0)
					args = append(args, command)
					if diffFrom, ok := params["diffFrom"]; ok && diffFrom.(string) != "" {
						args = append(args, fmt.Sprintf("--diff-from=\"%s\"", diffFrom))
					}
					if diffFromRemote, ok := params["diffFromRemote"]; ok && diffFromRemote.(string) != "" {
						args = append(args, fmt.Sprintf("--diff-from-remote=\"%s\"", diffFromRemote))
					}

					if tablePattern, ok := params["tablePattern"]; ok && tablePattern.(string) != "" {
						args = append(args, fmt.Sprintf("--tables=\"%s\"", tablePattern))
					}

					if schemaOnly, ok := params["schemaOnly"]; ok && schemaOnly.(bool) {
						args = append(args, "--schema=1")
					}

					if partitions, ok := params["partitions"]; ok && len(partitions.([]interface{})) > 0 {
						partitionsStr := make([]string, len(partitions.([]interface{})))
						for j, v := range partitions.([]interface{}) {
							partitionsStr[j] = fmt.Sprintf("--partitions=\"%s\"", v.(string))
						}
						args = append(args, partitionsStr...)
					}
					args = append(args, "--resumable=1", backupName)
					fullCommand := strings.Join(args, " ")
					log.Info().Str("operation", "ResumeOperationsAfterRestart").Send()
					commandId, _ := status.Current.Start(fullCommand)
					err, _ = api.metrics.ExecuteWithMetrics(command, 0, func() error {
						return api.cliApp.Run(append([]string{"clickhouse-backup", "-c", api.configPath, "--command-id", strconv.FormatInt(int64(commandId), 10)}, args...))
					})
					status.Current.Stop(commandId, err)
					if err != nil {
						return err
					}

					if err = os.Remove(stateFile); err != nil {
						if api.config.General.BackupsToKeepLocal >= 0 {
							return err
						}
						log.Warn().Str("operation", "ResumeOperationsAfterRestart").Msgf("remove %s return error: ", err)
					}
				default:
					return fmt.Errorf("unkown command for state file %s", stateFile)
				}
			}
		}
	}

	return nil
}

func (api *APIServer) getQueryParameter(q url.Values, paramName string) (string, bool) {
	paramNames := []string{strings.Replace(paramName, "-", "_", -1), strings.Replace(paramName, "_", "-", -1)}
	for _, name := range paramNames {
		if v, exists := q[name]; exists {
			return v[0], exists
		}
	}
	return "", false
}
