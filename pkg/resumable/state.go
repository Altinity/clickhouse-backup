package resumable

import (
	"fmt"
	apexLog "github.com/apex/log"
	"os"
	"path"
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

func (s *State) AppendToState(path string) {
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

func (s *State) IsAlreadyProcessed(path string) bool {
	s.mx.RLock()
	res := strings.Contains(s.currentState, path+"\n")
	s.mx.RUnlock()
	if res {
		s.log.Infof("%s already processed", path)
	}
	return res
}

func (s *State) Close() {
	_ = s.fp.Close()
}
