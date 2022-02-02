package metadata

import (
	"encoding/json"
	"io/ioutil"
)

func (tm *TableMetadata) Load(location string) (uint64, error) {
	data, err := ioutil.ReadFile(location)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(data, tm); err != nil {
		return 0, err
	}
	return uint64(len(data)), nil
}
