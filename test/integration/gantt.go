//go:build integration

package main

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/rs/zerolog/log"
)

const (
	ganttSlot        = 30 * time.Second
	envUsageSymbols  = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
	envUsageOverflow = '?'
	defaultTermWidth = 180
)

var envUsage = newEnvUsageRecorder()

type envUsageEvent struct {
	project string
	test    string
	start   time.Time
	finish  time.Time
}

type envUsageRecorder struct {
	mu      sync.Mutex
	active  map[string]envUsageEvent
	events  []envUsageEvent
	symbols map[string]rune
	warned  bool
}

type envUsageLegendItem struct {
	symbol   rune
	test     string
	duration time.Duration
}

func newEnvUsageRecorder() *envUsageRecorder {
	return &envUsageRecorder{
		active:  make(map[string]envUsageEvent),
		symbols: make(map[string]rune),
	}
}

func (r *envUsageRecorder) acquire(project, test string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.active[project] = envUsageEvent{
		project: project,
		test:    test,
		start:   time.Now(),
	}
	r.symbolForTest(test)
}

func (r *envUsageRecorder) release(project, test string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	event, ok := r.active[project]
	if !ok {
		return
	}
	if event.test != test {
		return
	}
	event.finish = time.Now()
	r.events = append(r.events, event)
	delete(r.active, project)
}

func (r *envUsageRecorder) printGantt() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for _, event := range r.active {
		event.finish = now
		r.events = append(r.events, event)
	}
	if len(r.events) == 0 {
		return
	}

	start := r.events[0].start
	finish := r.events[0].finish
	projects := make(map[string]struct{})
	for _, event := range r.events {
		if event.start.Before(start) {
			start = event.start
		}
		if event.finish.After(finish) {
			finish = event.finish
		}
		projects[event.project] = struct{}{}
		r.symbolForTest(event.test)
	}

	projectNames := make([]string, 0, len(projects))
	for project := range projects {
		projectNames = append(projectNames, project)
	}
	sort.Strings(projectNames)
	projectNameWidth := 0
	for _, project := range projectNames {
		if len(project) > projectNameWidth {
			projectNameWidth = len(project)
		}
	}

	width := int(finish.Sub(start)/ganttSlot) + 1
	rows := make(map[string][]rune, len(projectNames))
	for _, project := range projectNames {
		row := make([]rune, width)
		for i := range row {
			row[i] = ' '
		}
		rows[project] = row
	}
	for _, event := range r.events {
		from := int(event.start.Sub(start) / ganttSlot)
		to := int(event.finish.Sub(start) / ganttSlot)
		if to < from {
			to = from
		}
		row := rows[event.project]
		symbol := r.symbolForTest(event.test)
		for i := from; i <= to && i < len(row); i++ {
			row[i] = symbol
		}
	}

	var out strings.Builder
	out.WriteString("test environment usage Gantt chart, one character per 30 seconds:\n")
	for _, project := range projectNames {
		out.WriteString(fmt.Sprintf("%-*s %s\n", projectNameWidth, project, strings.TrimRight(string(rows[project]), " ")))
	}

	out.WriteString("\ntest environment usage legend:\n")
	out.WriteString(r.formatLegend(terminalWidth()))
	log.Info().Msg(strings.TrimRight(out.String(), "\n"))
}

func (r *envUsageRecorder) formatLegend(termWidth int) string {
	durations := make(map[string]time.Duration, len(r.symbols))
	for _, event := range r.events {
		durations[event.test] += event.finish.Sub(event.start)
	}

	items := make([]envUsageLegendItem, 0, len(r.symbols))
	testNameWidth := 0
	durationWidth := 0
	for test, symbol := range r.symbols {
		duration := durations[test].Round(time.Second).String()
		if len(test) > testNameWidth {
			testNameWidth = len(test)
		}
		if len(duration) > durationWidth {
			durationWidth = len(duration)
		}
		items = append(items, envUsageLegendItem{
			symbol:   symbol,
			test:     test,
			duration: durations[test],
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].duration == items[j].duration {
			return items[i].test < items[j].test
		}
		return items[i].duration > items[j].duration
	})

	itemWidth := 2 + testNameWidth + 2 + durationWidth
	columns := termWidth / (itemWidth + 2)
	if columns < 1 {
		columns = 1
	}

	var out strings.Builder
	for i, item := range items {
		if i > 0 {
			if i%columns == 0 {
				out.WriteByte('\n')
			} else {
				out.WriteString("  ")
			}
		}
		out.WriteString(fmt.Sprintf("%c %-*s %*s", item.symbol, testNameWidth, item.test, durationWidth, item.duration.Round(time.Second).String()))
	}
	return out.String()
}

func (r *envUsageRecorder) symbolForTest(test string) rune {
	if symbol, ok := r.symbols[test]; ok {
		return symbol
	}
	symbol, exhausted := envUsageSymbolAt(len(r.symbols))
	if exhausted && !r.warned {
		log.Warn().Msgf("test environment usage legend exceeded all one-character symbols, reusing %q", envUsageOverflow)
		r.warned = true
	}
	r.symbols[test] = symbol
	return symbol
}

// envUsageSymbolAt returns the idx-th distinct legend symbol: the 94 printable ASCII characters first,
// then unicode ranges which stay one terminal cell wide in monospace fonts (wide CJK and emoji would
// break the Gantt column alignment), ~900 symbols total before the overflow fallback
func envUsageSymbolAt(idx int) (rune, bool) {
	ascii := []rune(envUsageSymbols)
	if idx < len(ascii) {
		return ascii[idx], false
	}
	idx -= len(ascii)
	for _, block := range envUsageUnicodeBlocks {
		for symbol := block.lo; symbol <= block.hi; symbol++ {
			if !unicode.IsPrint(symbol) || unicode.IsSpace(symbol) || unicode.IsMark(symbol) {
				continue
			}
			if idx == 0 {
				return symbol, false
			}
			idx--
		}
	}
	return envUsageOverflow, true
}

var envUsageUnicodeBlocks = []struct{ lo, hi rune }{
	{0x00A1, 0x00FF}, // Latin-1 Supplement: ¡ ¢ £ … þ ÿ
	{0x0100, 0x024F}, // Latin Extended-A/B: Ā ā … ɏ
	{0x0391, 0x03C9}, // Greek: Α … Ω α … ω
	{0x0410, 0x044F}, // Cyrillic: А … Я а … я
	{0x2190, 0x21FF}, // Arrows: ← ↑ → ↓ …
	{0x2500, 0x25FF}, // Box Drawing + Geometric Shapes: ─ │ ┌ … ◿
}

func terminalWidth() int {
	if columns, err := strconv.Atoi(os.Getenv("COLUMNS")); err == nil && columns > 0 {
		return columns
	}
	if out, err := exec.Command("sh", "-c", "stty size < /dev/tty").Output(); err == nil {
		parts := strings.Fields(string(out))
		if len(parts) == 2 {
			if columns, err := strconv.Atoi(parts[1]); err == nil && columns > 0 {
				return columns
			}
		}
	}
	if out, err := exec.Command("tput", "cols").Output(); err == nil {
		if columns, err := strconv.Atoi(strings.TrimSpace(string(out))); err == nil && columns > 0 {
			return columns
		}
	}
	return defaultTermWidth
}
