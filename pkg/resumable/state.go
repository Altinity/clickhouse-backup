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
	fp           *os.File
	mx           *sync.RWMutex
}

func NewState(defaultDiskPath, backupName, command string) *State {
	s := State{
		stateFile:    path.Join(defaultDiskPath, "backup", backupName, fmt.Sprintf("%s.state", command)),
		currentState: "",
		mx:           &sync.RWMutex{},
	}
	fp, err := os.OpenFile(s.stateFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		apexLog.Warnf("can't open %s error: %v", s.stateFile, err)
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
			apexLog.Warnf("can't read %s error: %v", s.stateFile, err)
		} else {
			apexLog.Warnf("%s empty, will continue from scratch error: %v", s.stateFile, err)
		}
	}
	s.mx.Unlock()
}

func (s *State) AppendToState(path string) {
	s.mx.Lock()
	if s.fp != nil {
		_, err := s.fp.WriteString(path + "\n")
		if err != nil {
			apexLog.Warnf("can't write %s error: %v", s.stateFile, err)
		}
		s.fp.Sync()
	}
	s.currentState += path + "\n"
	s.mx.Unlock()
}

func (s *State) IsAlreadyProcessed(path string) bool {
	s.mx.RLock()
	res := strings.Contains(s.currentState, path+"\n")
	s.mx.RUnlock()
	if res {
		apexLog.Infof("%s already processed", path)
	}
	return res
}

func (s *State) Close() {
	_ = s.fp.Close()
}
