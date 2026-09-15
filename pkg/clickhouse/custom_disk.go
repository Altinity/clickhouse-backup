package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

// CustomDisk - a disk declared inline in table SETTINGS as `disk = disk(type = ..., ...)`,
// https://github.com/Altinity/clickhouse-backup/issues/943
// ClickHouse registers such a disk in system.disks under a generated `__tmp_internal_<sipHash128>` name
// (or under `name = '...'` when given); the credentials exist only in the table DDL.
type CustomDisk struct {
	// Args - `key = value` pairs in declaration order, values unquoted; the nested `disk = disk(...)` is not
	// stored here, a `disk = 'name'` / `disk = name` reference to another disk is stored as Args["disk"]
	Args map[string]string
	// Order - keys of Args in declaration order
	Order []string
	// Nested - inner `disk = disk(...)` of `cache` / `encrypted` wrappers, nil when `disk` references a disk by name
	Nested *CustomDisk
}

// Type - lower-cased `type` argument, `local` when absent (ClickHouse DiskFactory default)
func (d *CustomDisk) Type() string {
	if t, ok := d.Args["type"]; ok {
		return strings.ToLower(t)
	}
	return "local"
}

// Leaf - innermost disk of a `cache` / `encrypted` chain, the one which holds the object storage credentials
func (d *CustomDisk) Leaf() *CustomDisk {
	leaf := d
	for leaf.Nested != nil {
		leaf = leaf.Nested
	}
	return leaf
}

// customDiskRE - matches the `disk = disk(` MergeTree setting, `[HIDDEN]` masking keeps this part intact
var customDiskRE = regexp.MustCompile(`(?i)\bdisk\s*=\s*disk\s*\(`)

// HasCustomDisk - true when the CREATE/ATTACH TABLE query declares an inline `disk = disk(...)`
func HasCustomDisk(query string) bool {
	return customDiskRE.MatchString(query)
}

// ParseCustomDisk - extract and parse the `disk = disk(...)` setting from a CREATE/ATTACH TABLE query,
// returns nil, nil when the query has no inline disk
func ParseCustomDisk(query string) (*CustomDisk, error) {
	loc := customDiskRE.FindStringIndex(query)
	if loc == nil {
		return nil, nil
	}
	p := &customDiskParser{query: query, pos: loc[1]}
	disk, err := p.parseArgs()
	if err != nil {
		return nil, errors.Wrapf(err, "ParseCustomDisk: offset %d", p.pos)
	}
	return disk, nil
}

// customDiskParser - recursive descent parser of the `disk(...)` argument list, the grammar is
// comma separated `key = value` pairs, value is a single quoted string literal, a bare identifier/number
// or a nested `disk(...)` call, https://github.com/ClickHouse/ClickHouse/blob/master/src/Disks/getDiskConfigurationFromAST.cpp
type customDiskParser struct {
	query string
	pos   int
}

func (p *customDiskParser) skipSpaces() {
	for p.pos < len(p.query) {
		switch p.query[p.pos] {
		case ' ', '\t', '\r', '\n':
			p.pos++
		default:
			return
		}
	}
}

func (p *customDiskParser) eof() bool {
	return p.pos >= len(p.query)
}

// parseArgs - parse the argument list, p.pos points right after the opening parenthesis,
// on return p.pos points right after the matching closing parenthesis
func (p *customDiskParser) parseArgs() (*CustomDisk, error) {
	disk := &CustomDisk{Args: map[string]string{}}
	for {
		p.skipSpaces()
		if p.eof() {
			return nil, errors.New("unexpected end of query, `)` expected")
		}
		if p.query[p.pos] == ')' {
			p.pos++
			return disk, nil
		}
		key, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		key = strings.ToLower(key)
		p.skipSpaces()
		if p.eof() || p.query[p.pos] != '=' {
			return nil, errors.Errorf("`=` expected after `%s`", key)
		}
		p.pos++
		p.skipSpaces()
		if p.eof() {
			return nil, errors.Errorf("unexpected end of query, value of `%s` expected", key)
		}
		nested, value, err := p.parseValue()
		if err != nil {
			return nil, errors.Wrapf(err, "invalid value of `%s`", key)
		}
		if nested != nil {
			disk.Nested = nested
		} else {
			if _, exists := disk.Args[key]; !exists {
				disk.Order = append(disk.Order, key)
			}
			disk.Args[key] = value
		}
		p.skipSpaces()
		if p.eof() {
			return nil, errors.New("unexpected end of query, `,` or `)` expected")
		}
		switch p.query[p.pos] {
		case ',':
			p.pos++
		case ')':
			p.pos++
			return disk, nil
		default:
			return nil, errors.Errorf("`,` or `)` expected, got `%c`", p.query[p.pos])
		}
	}
}

// parseValue - a nested `disk(...)` call, a single quoted string literal, or a bare identifier / number
func (p *customDiskParser) parseValue() (*CustomDisk, string, error) {
	if p.query[p.pos] == '\'' {
		value, err := p.parseStringLiteral()
		return nil, value, err
	}
	start := p.pos
	ident, err := p.parseIdentifier()
	if err != nil {
		return nil, "", err
	}
	savedPos := p.pos
	p.skipSpaces()
	if !p.eof() && p.query[p.pos] == '(' && strings.HasPrefix(strings.ToLower(ident), "disk") {
		p.pos++
		nested, nestedErr := p.parseArgs()
		if nestedErr != nil {
			return nil, "", nestedErr
		}
		return nested, "", nil
	}
	p.pos = savedPos
	return nil, p.query[start:p.pos], nil
}

// parseIdentifier - a bare identifier or a number, ClickHouse requires keys to be identifiers and converts
// identifier values to strings via evaluateConstantExpressionOrIdentifierAsLiteral
func (p *customDiskParser) parseIdentifier() (string, error) {
	start := p.pos
	for p.pos < len(p.query) {
		c := p.query[p.pos]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-' || c == '$' {
			p.pos++
			continue
		}
		break
	}
	if p.pos == start {
		return "", errors.Errorf("identifier expected, got `%c`", p.query[p.pos])
	}
	return p.query[start:p.pos], nil
}

// parseStringLiteral - single quoted literal, a quote is escaped by a backslash or by doubling it, a backslash escapes the next character
func (p *customDiskParser) parseStringLiteral() (string, error) {
	p.pos++
	var sb strings.Builder
	for p.pos < len(p.query) {
		c := p.query[p.pos]
		switch c {
		case '\\':
			if p.pos+1 >= len(p.query) {
				return "", errors.New("unexpected end of query inside string literal")
			}
			sb.WriteByte(unescapeChar(p.query[p.pos+1]))
			p.pos += 2
		case '\'':
			if p.pos+1 < len(p.query) && p.query[p.pos+1] == '\'' {
				sb.WriteByte('\'')
				p.pos += 2
				continue
			}
			p.pos++
			return sb.String(), nil
		default:
			sb.WriteByte(c)
			p.pos++
		}
	}
	return "", errors.New("unterminated string literal")
}

// unescapeChar - ClickHouse backslash escape sequences, unknown sequences yield the character itself
func unescapeChar(c byte) byte {
	switch c {
	case 'b':
		return '\b'
	case 'f':
		return '\f'
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	case '0':
		return 0
	case 'a':
		return '\a'
	case 'v':
		return '\v'
	}
	return c
}

// TmpInternalDiskPrefix - DiskSelector::TMP_INTERNAL_DISK_PREFIX, name prefix of a `disk(...)` disk declared without `name = '...'`,
// the suffix is sipHash128 of the canonical disk declaration
const TmpInternalDiskPrefix = "__tmp_internal_"

// TmpStoragePolicyPrefix - StoragePolicySelector::TMP_STORAGE_POLICY_PREFIX, a table with `disk = ...` gets a
// synthesized single-disk storage policy named `__<disk name>`
const TmpStoragePolicyPrefix = "__"

// GetTableDiskNameFromStoragePolicy - resolve the disk a `SETTINGS disk = ...` table lives on via
// system.tables.storage_policy = "__" + disk name; returns "" for tables which use a regular storage policy
func (ch *ClickHouse) GetTableDiskNameFromStoragePolicy(ctx context.Context, database, table string) (string, error) {
	var storagePolicy string
	query := "SELECT storage_policy FROM system.tables WHERE database=? AND name=?"
	if err := ch.SelectSingleRow(ctx, &storagePolicy, query, database, table); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", errors.Wrapf(err, "GetTableDiskNameFromStoragePolicy: %s.%s", database, table)
	}
	if !strings.HasPrefix(storagePolicy, TmpStoragePolicyPrefix) {
		return "", nil
	}
	return strings.TrimPrefix(storagePolicy, TmpStoragePolicyPrefix), nil
}

// CustomDiskTableLike - `system.tables.create_table_query` fragment which marks a table with an inline disk
const CustomDiskTableLike = "%disk = disk(%"

// GetCustomDiskTables - tables which declare an inline `SETTINGS disk = disk(...)`, their DDL is the only source of
// credentials for such disks, https://github.com/Altinity/clickhouse-backup/issues/943
// Tables whose DDL is still masked after the `metadata/<db>/<table>.sql` fallback are returned together with a
// non nil error, the caller decides whether missing credentials for those tables are fatal.
func (ch *ClickHouse) GetCustomDiskTables(ctx context.Context) ([]Table, error) {
	settings := map[string]bool{"format_display_secrets_in_show_and_select": false}
	var err error
	if settings, err = ch.CheckSettingsExists(ctx, settings); err != nil {
		return nil, errors.Wrap(err, "GetCustomDiskTables: check settings")
	}
	isStoragePolicyPresent := make([]struct {
		IsPresent uint64 `ch:"is_present"`
	}, 0)
	isFieldPresentSQL := "SELECT countIf(name='storage_policy') AS is_present FROM system.columns WHERE database='system' AND table='tables'"
	if err = ch.SelectContext(ctx, &isStoragePolicyPresent, isFieldPresentSQL); err != nil {
		return nil, errors.Wrap(err, "GetCustomDiskTables: check system.tables.storage_policy")
	}
	customDiskTablesSQL := "SELECT database, name, engine, create_table_query"
	if len(isStoragePolicyPresent) > 0 && isStoragePolicyPresent[0].IsPresent > 0 {
		customDiskTablesSQL += ", storage_policy"
	}
	customDiskTablesSQL += fmt.Sprintf(" FROM system.tables WHERE is_temporary = 0 AND create_table_query LIKE '%s'", CustomDiskTableLike)
	customDiskTablesSQL = ch.addSettingsSQL(customDiskTablesSQL, settings)
	tables := make([]Table, 0)
	if err = ch.SelectContext(ctx, &tables, customDiskTablesSQL); err != nil {
		return nil, errors.Wrap(err, "GetCustomDiskTables: select tables")
	}
	if len(tables) == 0 {
		return tables, nil
	}
	metadataPath, err := ch.getMetadataPath(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "GetCustomDiskTables: get metadata path")
	}
	maskedTables := make([]string, 0)
	for i := range tables {
		if !strings.Contains(tables[i].CreateTableQuery, "'[HIDDEN]'") {
			continue
		}
		tables[i].CreateTableQuery = ch.unmaskCreateTableQueryFromMetadata(tables[i], metadataPath)
		if strings.Contains(tables[i].CreateTableQuery, "'[HIDDEN]'") {
			maskedTables = append(maskedTables, fmt.Sprintf("%s.%s", tables[i].Database, tables[i].Name))
		}
	}
	if len(maskedTables) > 0 {
		return tables, errors.Errorf(
			"GetCustomDiskTables: `disk = disk(...)` credentials of %s are masked as '[HIDDEN]', "+
				"run clickhouse-backup on the clickhouse-server host so `metadata/<db>/<table>.sql` is readable, "+
				"or set `display_secrets_in_show_and_select=1` in clickhouse-server config and grant `displaySecretsInShowAndSelect` to the backup user",
			strings.Join(maskedTables, ", "),
		)
	}
	return tables, nil
}
