package metadata

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"os"
)

func (tm *TableMetadata) Load(location string) (uint64, error) {
	data, err := os.ReadFile(location)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(data, tm); err != nil {
		return 0, err
	}
	log.Debug().Str("operation", "TableMetadata.Load").Msgf("success %s", location)
	return uint64(len(data)), nil
}
