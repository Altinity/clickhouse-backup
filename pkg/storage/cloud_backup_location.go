package storage

import (
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

// CloudBackupLocation - `<base_backup>` of a native BACKUP, e.g. S3('url','key','secret'),
// S3('url', extra_credentials(role_arn='...')) or AzureBlobStorage('connection_string','container','blob_path'),
// see BackupInfo::toAST in ClickHouse
type CloudBackupLocation struct {
	Engine string
	// Args - positional string literal arguments
	Args []string
	// KV - key=value arguments, e.g. url='...'
	KV map[string]string
	// Secrets - credentials found in the arguments, to redact them from logs and errors
	Secrets []string
}

var cloudAccountKeyRE = regexp.MustCompile(`(?i)AccountKey=([^;]+)`)

// ParseCloudBackupLocation parses BackupInfo::toString() output, identifiers (named collections) and nested
// functions (extra_credentials) are skipped
func ParseCloudBackupLocation(s string) (*CloudBackupLocation, error) {
	s = strings.TrimSpace(s)
	open := strings.IndexByte(s, '(')
	if open <= 0 || !strings.HasSuffix(s, ")") {
		return nil, errors.New("can't parse <base_backup>, expected Engine(...)")
	}
	loc := &CloudBackupLocation{Engine: strings.TrimSpace(s[:open]), KV: map[string]string{}}
	for _, arg := range splitCloudBackupArgs(s[open+1:len(s)-1], ',') {
		arg = strings.TrimSpace(arg)
		if strings.HasPrefix(arg, "'") {
			value, err := unquoteCloudBackupArg(arg)
			if err != nil {
				return nil, err
			}
			loc.Args = append(loc.Args, value)
			continue
		}
		if kv := splitCloudBackupArgs(arg, '='); len(kv) == 2 && strings.HasPrefix(strings.TrimSpace(kv[1]), "'") {
			value, err := unquoteCloudBackupArg(strings.TrimSpace(kv[1]))
			if err != nil {
				return nil, err
			}
			loc.KV[strings.ToLower(strings.TrimSpace(kv[0]))] = value
		}
	}
	// everything except the location itself may hold credentials
	locationArgs := 1
	if loc.IsAzureBlob() {
		locationArgs = 3
		for _, v := range append(append([]string{}, loc.Args...), loc.KV["connection_string"]) {
			if m := cloudAccountKeyRE.FindStringSubmatch(v); m != nil {
				loc.Secrets = append(loc.Secrets, m[1])
			}
		}
	}
	for i, v := range loc.Args {
		if i >= locationArgs && v != "" {
			loc.Secrets = append(loc.Secrets, v)
		}
	}
	for k, v := range loc.KV {
		if k != "url" && k != "container" && k != "blob_path" && k != "connection_string" && v != "" {
			loc.Secrets = append(loc.Secrets, v)
		}
	}
	return loc, nil
}

func (loc *CloudBackupLocation) IsAzureBlob() bool {
	return strings.EqualFold(loc.Engine, "AzureBlobStorage")
}

func (loc *CloudBackupLocation) arg(key string, pos int) string {
	if v, ok := loc.KV[key]; ok {
		return v
	}
	if pos < len(loc.Args) {
		return loc.Args[pos]
	}
	return ""
}

// KeyIn returns the key prefix of the backup inside bucket (S3) or container (AzureBlobStorage),
// false when the backup is stored in another bucket/container or the location is unknown
func (loc *CloudBackupLocation) KeyIn(bucketOrContainer string) (string, bool) {
	if bucketOrContainer == "" {
		return "", false
	}
	if loc.IsAzureBlob() {
		if loc.arg("container", 1) != bucketOrContainer {
			return "", false
		}
		key := strings.Trim(loc.arg("blob_path", 2), "/")
		return key, key != ""
	}
	if !strings.EqualFold(loc.Engine, "S3") {
		return "", false
	}
	u, err := url.Parse(loc.arg("url", 0))
	if err != nil || u.Host == "" {
		return "", false
	}
	var key string
	switch {
	// s3://bucket/key, gs://bucket/key
	case u.Scheme != "http" && u.Scheme != "https":
		if u.Host != bucketOrContainer {
			return "", false
		}
		key = u.Path
	// virtual-hosted style https://bucket.s3.region.amazonaws.com/key
	case strings.HasPrefix(u.Host, bucketOrContainer+"."):
		key = u.Path
	// path style https://s3.region.amazonaws.com/bucket/key, https://storage.googleapis.com/bucket/key, http://minio:9000/bucket/key
	case strings.HasPrefix(u.Path, "/"+bucketOrContainer+"/"):
		key = strings.TrimPrefix(u.Path, "/"+bucketOrContainer)
	default:
		return "", false
	}
	key = strings.Trim(key, "/")
	return key, key != ""
}

// String - the location without credentials, safe for logs and `list` output
func (loc *CloudBackupLocation) String() string {
	if loc.IsAzureBlob() {
		return "azblob://" + path.Join(loc.arg("container", 1), loc.arg("blob_path", 2))
	}
	u, err := url.Parse(loc.arg("url", 0))
	if err != nil {
		return loc.Engine + "(?)"
	}
	u.User = nil
	u.RawQuery = ""
	return u.String()
}

// splitCloudBackupArgs splits by sep outside of quotes and parentheses
func splitCloudBackupArgs(s string, sep byte) []string {
	parts := make([]string, 0)
	depth, start := 0, 0
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inQuote && c == '\\':
			i++
		case c == '\'':
			inQuote = !inQuote
		case inQuote:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == sep && depth == 0:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

// unquoteCloudBackupArg reverts writeQuotedString of ClickHouse: backslash escapes and doubled quotes
func unquoteCloudBackupArg(s string) (string, error) {
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return "", errors.New("can't parse <base_backup>, bad string literal")
	}
	s = s[1 : len(s)-1]
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case 'n':
				out.WriteByte('\n')
			case 't':
				out.WriteByte('\t')
			case 'r':
				out.WriteByte('\r')
			case '0':
				out.WriteByte(0)
			default:
				out.WriteByte(s[i])
			}
			continue
		}
		if c == '\'' && i+1 < len(s) && s[i+1] == '\'' {
			i++
		}
		out.WriteByte(c)
	}
	return out.String(), nil
}
