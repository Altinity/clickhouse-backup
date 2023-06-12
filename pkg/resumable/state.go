package resumable

import (
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
)

type State struct {
	stateFile    string
	currentState string
	params       map[string]interface{}
	logger       zerolog.Logger
	fp           *os.File
	mx           *sync.RWMutex
}

func NewState(defaultDiskPath, backupName, command string, params map[string]interface{}) *State {
	s := State{
		stateFile:    path.Join(defaultDiskPath, "backup", backupName, fmt.Sprintf("%s.state", command)),
		currentState: "",
		mx:           &sync.RWMutex{},
		logger:       log.With().Str("logger", "resumable").Logger(),
	}
	fp, err := os.OpenFile(s.stateFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		s.logger.Warn().Msgf("can't open %s error: %v", s.stateFile, err)
	}
	s.fp = fp
	s.LoadState()
	s.LoadParams()
	if len(s.params) == 0 && params != nil {
		s.params = params
		if paramsBytes, err := json.Marshal(s.params); err == nil {
			s.AppendToState(string(paramsBytes), 0)
		}
	}
	return &s
}

func (s *State) GetParams() map[string]interface{} {
	return s.params
}

func (s *State) LoadParams() {
	lines := strings.SplitN(s.currentState, "\n", 2)
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "{") {
		return
	}
	//size 0 during write
	lines[0] = strings.TrimSuffix(lines[0], ":0")
	if err := json.Unmarshal([]byte(lines[0]), &s.params); err != nil {
		s.logger.Error().Msgf("can't parse state file line 0 as []interface{}: %s", lines[0])
	}
}

func (s *State) LoadState() {
	s.mx.Lock()
	state, err := os.ReadFile(s.stateFile)
	if err == nil {
		s.currentState = string(state)
	} else {
		s.currentState = ""
		if !os.IsNotExist(err) {
			s.logger.Warn().Msgf("can't read %s error: %v", s.stateFile, err)
		} else {
			s.logger.Warn().Msgf("%s empty, will continue from scratch error: %v", s.stateFile, err)
		}
	}
	s.mx.Unlock()
}

func (s *State) AppendToState(path string, size int64) {
	path = fmt.Sprintf("%s:%d", path, size)
	s.mx.Lock()
	if s.fp != nil {
		_, err := s.fp.WriteString(path + "\n")
		if err != nil {
			s.logger.Warn().Msgf("can't write %s error: %v", s.stateFile, err)
		}
		err = s.fp.Sync()
		if err != nil {
			s.logger.Warn().Msgf("can't sync %s error: %v", s.stateFile, err)
		}
	}
	s.currentState += path + "\n"
	s.mx.Unlock()
}

func (s *State) IsAlreadyProcessedBool(path string) bool {
	isProcesses, _ := s.IsAlreadyProcessed(path)
	return isProcesses
}
func (s *State) IsAlreadyProcessed(path string) (bool, int64) {
	var size int64
	var err error
	s.mx.RLock()
	res := strings.Index(s.currentState, path+":")
	if res >= 0 {
		s.logger.Info().Msgf("%s already processed", path)
		sSize := s.currentState[res : res+strings.Index(s.currentState[res:], "\n")]
		sSize = sSize[strings.Index(sSize, ":")+1:]
		size, err = strconv.ParseInt(sSize, 10, 64)
		if err != nil {
			s.logger.Warn().Msgf("invalid size %s in upload state: %v", sSize, err)
		}
	}
	s.mx.RUnlock()
	return res >= 0, size
}

func (s *State) Close() {
	_ = s.fp.Close()
}
