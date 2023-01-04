package resumable

import (
	"fmt"
	apexLog "github.com/apex/log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

type State struct {
	stateFile    string
	currentState string
	log          *apexLog.Entry
	fp           *os.File
	mx           *sync.RWMutex
}

func NewState(defaultDiskPath, backupName, command string) *State {
	s := State{
		stateFile:    path.Join(defaultDiskPath, "backup", backupName, fmt.Sprintf("%s.state", command)),
		currentState: "",
		mx:           &sync.RWMutex{},
		log:          apexLog.WithField("logger", "resumable"),
	}
	fp, err := os.OpenFile(s.stateFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		s.log.Warnf("can't open %s error: %v", s.stateFile, err)
	}
	s.fp = fp
	s.LoadState()
	return &s
}

func (s *State) LoadState() {
	s.mx.Lock()
	state, err := os.ReadFile(s.stateFile)
	if err == nil {
		s.currentState = string(state)
	} else {
		s.currentState = ""
		if !os.IsNotExist(err) {
			s.log.Warnf("can't read %s error: %v", s.stateFile, err)
		} else {
			s.log.Warnf("%s empty, will continue from scratch error: %v", s.stateFile, err)
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
			s.log.Warnf("can't write %s error: %v", s.stateFile, err)
		}
		err = s.fp.Sync()
		if err != nil {
			s.log.Warnf("can't sync %s error: %v", s.stateFile, err)
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
		s.log.Infof("%s already processed", path)
		sSize := s.currentState[res : res+strings.Index(s.currentState[res:], "\n")]
		size, err = strconv.ParseInt(sSize, 10, 64)
		if err != nil {
			s.log.Warnf("invalid size %s in upload state: %v", sSize, err)
		}
	}
	s.mx.RUnlock()
	return res >= 0, size
}

func (s *State) Close() {
	_ = s.fp.Close()
}
