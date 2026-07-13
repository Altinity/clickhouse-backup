package config

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
)

// WatchCronParser - accept standard 5 fields crontab expression, optional leading seconds field and @every/@daily descriptors
var WatchCronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// watchScheduleKeyRE - split key=value pairs by known keys instead of comma, cause cron expressions could contain commas
var watchScheduleKeyRE = regexp.MustCompile(`(^|,)(name|full|increment|full_type|delete_previous_cycle)=`)
var watchScheduleNameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// WatchSchedule - named cron driven watch chain, alternative to watch_interval/full_interval, see https://github.com/Altinity/clickhouse-backup/issues/1354
type WatchSchedule struct {
	Name                string `yaml:"name"`
	Full                string `yaml:"full"`
	Increment           string `yaml:"increment"`
	FullType            string `yaml:"full_type"`
	DeletePreviousCycle bool   `yaml:"delete_previous_cycle"`
}

type WatchSchedules []WatchSchedule

// Decode - implements envconfig.Decoder, schedules separated by ';' cause cron expressions and key=value list contain commas
func (ws *WatchSchedules) Decode(value string) error {
	if strings.TrimSpace(value) == "" {
		*ws = nil
		return nil
	}
	parsed, err := ParseWatchSchedules(strings.Split(value, ";"))
	if err != nil {
		return err
	}
	*ws = parsed
	return nil
}

func (s *WatchSchedule) Validate() error {
	if s.Name == "" {
		return errors.New("watch schedule requires non-empty `name`")
	}
	if !watchScheduleNameRE.MatchString(s.Name) {
		return errors.Errorf("invalid watch schedule name `%s`, only [a-zA-Z0-9_-] allowed", s.Name)
	}
	if s.Full == "" {
		return errors.Errorf("watch schedule `%s` requires `full` cron expression", s.Name)
	}
	if _, err := WatchCronParser.Parse(s.Full); err != nil {
		return errors.Wrapf(err, "watch schedule `%s`, invalid `full` cron expression `%s`", s.Name, s.Full)
	}
	if s.Increment != "" {
		if _, err := WatchCronParser.Parse(s.Increment); err != nil {
			return errors.Wrapf(err, "watch schedule `%s`, invalid `increment` cron expression `%s`", s.Name, s.Increment)
		}
	}
	switch s.FullType {
	case "":
		s.FullType = "create"
	case "create", "rebase":
	default:
		return errors.Errorf("watch schedule `%s`, invalid `full_type` `%s`, expect `create` or `rebase`", s.Name, s.FullType)
	}
	return nil
}

func (ws WatchSchedules) Validate() error {
	for i := range ws {
		if err := ws[i].Validate(); err != nil {
			return err
		}
		for j := range ws {
			if i == j {
				continue
			}
			if ws[i].Name == ws[j].Name {
				return errors.Errorf("duplicate watch schedule name `%s`", ws[i].Name)
			}
			// name works as backup name prefix, prefix of another name breaks chain isolation
			if strings.HasPrefix(ws[j].Name+"-", ws[i].Name+"-") {
				return errors.Errorf("watch schedule name `%s` is a prefix of `%s`, backup chains will overlap", ws[i].Name, ws[j].Name)
			}
		}
	}
	return nil
}

// ParseWatchSchedule - parse `name=<name>,full=<cron>[,increment=<cron>][,full_type=create|rebase][,delete_previous_cycle=true|false]`
func ParseWatchSchedule(param string) (WatchSchedule, error) {
	s := WatchSchedule{}
	param = strings.TrimSpace(param)
	matches := watchScheduleKeyRE.FindAllStringSubmatchIndex(param, -1)
	if len(matches) == 0 || matches[0][0] != 0 {
		return s, errors.Errorf("invalid schedule `%s`, expect name=<name>,full=<cron>[,increment=<cron>][,full_type=create|rebase][,delete_previous_cycle=true|false]", param)
	}
	seen := map[string]bool{}
	for i, m := range matches {
		key := param[m[4]:m[5]]
		if seen[key] {
			return s, errors.Errorf("invalid schedule `%s`, duplicate key `%s`", param, key)
		}
		seen[key] = true
		valueEnd := len(param)
		if i+1 < len(matches) {
			valueEnd = matches[i+1][0]
		}
		value := strings.TrimSpace(param[m[1]:valueEnd])
		switch key {
		case "name":
			s.Name = value
		case "full":
			s.Full = value
		case "increment":
			s.Increment = value
		case "full_type":
			s.FullType = value
		case "delete_previous_cycle":
			switch strings.ToLower(value) {
			case "true", "1", "yes":
				s.DeletePreviousCycle = true
			case "false", "0", "no":
				s.DeletePreviousCycle = false
			default:
				return s, errors.Errorf("invalid schedule `%s`, `delete_previous_cycle` expects true or false, got `%s`", param, value)
			}
		}
	}
	return s, s.Validate()
}

func ParseWatchSchedules(params []string) (WatchSchedules, error) {
	ws := make(WatchSchedules, 0, len(params))
	for _, param := range params {
		if strings.TrimSpace(param) == "" {
			continue
		}
		s, err := ParseWatchSchedule(param)
		if err != nil {
			return nil, err
		}
		ws = append(ws, s)
	}
	if err := ws.Validate(); err != nil {
		return nil, err
	}
	return ws, nil
}
