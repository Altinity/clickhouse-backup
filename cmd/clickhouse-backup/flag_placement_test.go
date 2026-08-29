package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// urfave/cli v1 stopped flag parsing at the first positional argument, so
// `clickhouse-backup download my_backup --tables db.t` silently downloaded
// everything. v3 parses flags anywhere, see
// https://github.com/Altinity/clickhouse-backup/issues/1459
func TestFlagsAreParsedAfterPositionalArguments(t *testing.T) {
	cases := []struct {
		name  string
		args  []string
		check func(r *require.Assertions, cmd *cli.Command)
	}{
		{
			name: "tables after the backup name",
			args: []string{"download", "my_backup", "--tables", "db.t"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal("my_backup", cmd.Args().First())
				r.Equal("db.t", cmd.String("tables"))
			},
		},
		{
			name: "tables before the backup name still works",
			args: []string{"download", "--tables", "db.t", "my_backup"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal("my_backup", cmd.Args().First())
				r.Equal("db.t", cmd.String("tables"))
			},
		},
		{
			name: "--flag=value after the backup name",
			args: []string{"download", "my_backup", "--tables=db.t"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal("my_backup", cmd.Args().First())
				r.Equal("db.t", cmd.String("tables"))
			},
		},
		{
			name: "short alias after the backup name",
			args: []string{"download", "my_backup", "-t", "db.t"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal("my_backup", cmd.Args().First())
				// main.go reads this flag through all three spellings
				r.Equal("db.t", cmd.String("t"))
				r.Equal("db.t", cmd.String("tables"))
				r.Equal("db.t", cmd.String("table"))
			},
		},
		{
			name: "bool flag after the backup name",
			args: []string{"restore", "my_backup", "--rm"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal("my_backup", cmd.Args().First())
				r.True(cmd.Bool("rm"))
				r.True(cmd.Bool("drop"))
			},
		},
		{
			name: "repeated slice flag after the backup name",
			args: []string{"create", "my_backup", "--partitions", "p1", "--partitions", "p2"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal("my_backup", cmd.Args().First())
				r.Equal([]string{"p1", "p2"}, cmd.StringSlice("partitions"))
			},
		},
		{
			name: "persistent global flag after the backup name",
			args: []string{"download", "my_backup", "-c", "/tmp/other-config.yml"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal("my_backup", cmd.Args().First())
				r.Equal("/tmp/other-config.yml", cmd.String("config"))
			},
		},
		{
			name: "-- stops flag parsing",
			args: []string{"restore", "my_backup", "--", "--rm"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal([]string{"my_backup", "--rm"}, cmd.Args().Slice())
				r.False(cmd.Bool("rm"))
			},
		},
		{
			// urfave/cli v3 splits slice values on "," where v1 appended them verbatim.
			// Fragmenting the tuple syntax here would silently back up the wrong partitions.
			name: "partition tuple syntax survives as one element",
			args: []string{"create", "my_backup", "--partitions=(0,'2022-01-02'),(0,'2022-01-03')"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal([]string{"(0,'2022-01-02'),(0,'2022-01-03')"}, cmd.StringSlice("partitions"))
			},
		},
		{
			// splitting this one drops the `db.t:` scope from p2 and widens it to *.*
			name: "per-table partition list survives as one element",
			args: []string{"create", "my_backup", "--partitions", "db.t:p1,p2"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal([]string{"db.t:p1,p2"}, cmd.StringSlice("partitions"))
			},
		},
		{
			name: "env override value keeps its commas",
			args: []string{"download", "my_backup", "--env", "CLICKHOUSE_SKIP_TABLES=a.*,b.*"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal([]string{"CLICKHOUSE_SKIP_TABLES=a.*,b.*"}, cmd.StringSlice("env"))
			},
		},
		{
			name: "watch schedule spec survives as one element",
			args: []string{"watch", "--schedule", "name=daily,full=0 0 * * *,increment=0 * * * *"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal([]string{"name=daily,full=0 0 * * *,increment=0 * * * *"}, cmd.StringSlice("schedule"))
			},
		},
		{
			name: "sub-command arguments keep their order",
			args: []string{"delete", "local", "my_backup", "--force"},
			check: func(r *require.Assertions, cmd *cli.Command) {
				r.Equal("local", cmd.Args().Get(0))
				r.Equal("my_backup", cmd.Args().Get(1))
				r.True(cmd.Bool("force"))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			cmd, err := runWithStubbedActions(t, tc.args)
			r.NoError(err)
			r.NotNil(cmd, "action of %q was never reached", tc.args[0])
			tc.check(r, cmd)
		})
	}
}

// runWithStubbedActions runs the real command tree from newRootCommand with every
// action replaced by a capture, so parsing is exercised against the production flag
// definitions without opening a ClickHouse connection.
func runWithStubbedActions(t *testing.T, args []string) (*cli.Command, error) {
	t.Helper()

	root := newRootCommand()
	var parsed *cli.Command
	capture := func(_ context.Context, cmd *cli.Command) error {
		parsed = cmd
		return nil
	}
	for _, cmd := range root.Commands {
		cmd.Action = capture
	}
	err := root.Run(context.Background(), append([]string{"clickhouse-backup"}, args...))
	return parsed, err
}
