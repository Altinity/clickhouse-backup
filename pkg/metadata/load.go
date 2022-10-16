package metadata

import (
	"encoding/json"
	apexLog "github.com/apex/log"
	"os"
)

func (tm *TableMetadata) Load(location string) (uint64, error) {
	log := apexLog.WithField("logger", "metadata.Load")
	data, err := os.ReadFile(location)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(data, tm); err != nil {
		return 0, err
	}
	log.Debugf("success %s", location)
	return uint64(len(data)), nil
}
