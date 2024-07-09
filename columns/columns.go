// Copyright 2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package columns

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/mattn/go-isatty"
	terminal "golang.org/x/term"
)

type columnRow struct {
	kind   int
	values []any
}

type Writer struct {
	heading     string
	rows        []*columnRow
	sep         string
	indent      string
	colorScheme string
}

var colsStyles = map[string]text.Color{
	"yellow":  text.FgYellow,
	"blue":    text.FgBlue,
	"cyan":    text.FgCyan,
	"green":   text.FgGreen,
	"magenta": text.FgMagenta,
	"red":     text.FgRed,
}

func New(heading string, a ...any) *Writer {
	w := &Writer{sep: ":"}
	w.SetHeading(heading, a...)

	return w
}

const (
	kindRow    = 1
	kindIndent = 2
	kindLine   = 3
	kindTitle  = 4
)

// SetColorScheme sets a color schema to use. One of yellow, blue, cyan, green, magenta or red - rest treated as no color
func (w *Writer) SetColorScheme(s string) {
	w.colorScheme = s
}

// SetSeparator sets the separator to use after headings, defaults to :
func (w *Writer) SetSeparator(seq string) {
	w.sep = seq
}

// Frender renders to the writer 0
func (w *Writer) Frender(o io.Writer) error {
	// figure out the right most edge of first column
	longest := 0
	for _, row := range w.rows {
		if row.kind == 1 && len(row.values) > 0 && len(row.values[0].(string)) > longest {
			longest = len(row.values[0].(string))
		}
	}

	if w.IsTerminal(o) {
		color, ok := colsStyles[w.colorScheme]
		if ok {
			w.sep = color.Sprint(":")
		}
	}

	if w.heading != "" {
		fmt.Fprintln(o, w.heading)
		fmt.Fprintln(o)
	}

	prev := -1
	prevEmpty := false

	for i, row := range w.rows {
		switch row.kind {
		case kindIndent:
			w.indent = row.values[0].(string)

		case kindTitle:
			if (i != 0 && prev != kindTitle && prev != kindLine) || prev == kindLine && !prevEmpty {
				fmt.Fprintln(o)
			}
			fmt.Fprintln(o, w.indent+w.maybeAddColon(o, row.values[0].(string), false))
			fmt.Fprintln(o)
			prev = row.kind
			prevEmpty = false

		case kindRow:
			left := row.values[0].(string)
			padding := longest - utf8StringLen(left) + 2
			if padding < 0 {
				padding = 0
			}

			if left == "" {
				// when left is empty we assume it's a multi line continuation so no : in-front
				fmt.Fprintf(o, "%s%s %v\n", w.indent, strings.Repeat(" ", padding+1), row.values[1])
			} else {
				fmt.Fprintf(o, "%s%s%s%s %v\n", w.indent, strings.Repeat(" ", padding), left, w.sep, row.values[1])
			}
			prev = row.kind
			prevEmpty = false

		case kindLine:
			// avoid 2 blank lines
			if prev == kindTitle && len(row.values) == 0 {
				continue
			}

			fmt.Fprintln(o, append([]any{w.indent}, row.values...)...)
			prev = row.kind
			prevEmpty = len(row.values) == 0
		}
	}

	return nil
}

// Render produce the result as a string
func (w *Writer) Render() (string, error) {
	buf := bytes.NewBuffer([]byte{})
	err := w.Frender(buf)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

// AddRowUnlimitedIf puts "unlimited" as a value if unlimited is true
func (w *Writer) AddRowUnlimitedIf(t string, v any, unlimited bool) {
	if unlimited {
		w.AddRow(t, "unlimited")
	} else {
		w.AddRow(t, v)
	}
}

// AddRowUnlimited puts "unlimited" as a value when v == unlimited
func (w *Writer) AddRowUnlimited(t string, v int64, unlimited int64) {
	if v == unlimited {
		w.AddRow(t, "unlimited")
	} else {
		w.AddRow(t, v)
	}
}

// AddRow adds a row, v will be formatted if time.Time, time.Duration, []string, floats, ints and uints
func (w *Writer) AddRow(t string, v any) {
	w.rows = append(w.rows, &columnRow{kind: kindRow, values: []any{strings.TrimSuffix(t, w.sep), F(v)}})
}

// AddRowIf adds a row if the condition is true
func (w *Writer) AddRowIf(t string, v any, condition bool) {
	if !condition {
		return
	}
	w.AddRow(t, v)
}

// AddRowIfNotEmpty adds a row if v is not an empty string
func (w *Writer) AddRowIfNotEmpty(t string, v string) {
	if v == "" {
		return
	}
	w.AddRow(t, v)
}

// AddRowf adds a row with printf like behavior on the value, no auto formatting of values will be done like in AddRow()
func (w *Writer) AddRowf(t string, format string, a ...any) {
	w.AddRow(t, fmt.Sprintf(format, a...))
}

// AddSectionTitle adds a new section
func (w *Writer) AddSectionTitle(format string, a ...any) {
	w.rows = append(w.rows, &columnRow{kind: kindTitle, values: []any{fmt.Sprintf(format, a...)}})
}

// SetHeading sets the initial heading
func (w *Writer) SetHeading(format string, a ...any) {
	w.heading = fmt.Sprintf(format, a...)
}

// Println Adds a line to the report rendered outside of columnar layout
func (w *Writer) Println(msg ...string) {
	var val []any
	for _, v := range msg {
		val = append(val, v)
	}
	w.rows = append(w.rows, &columnRow{kind: kindLine, values: val})
}

// AddMapIntsAsValue adds a row with title t and the data as value. Optionally sorts by value.
func (w *Writer) AddMapIntsAsValue(t string, data map[string]int, sortValues bool, reverse bool) {
	var list []string
	for k := range data {
		list = append(list, k)
	}

	if sortValues {
		sort.Slice(list, func(i, j int) bool {
			if reverse {
				return data[list[i]] > data[list[j]]
			} else {
				return data[list[i]] < data[list[j]]
			}
		})
	}

	for i, k := range list {
		if i == 0 {
			w.AddRowf(t, "%s: %s", k, F(data[k]))
		} else {
			w.AddRowf("", "%s: %s", k, F(data[k]))
		}
	}
}

// AddStringsAsValue adds multiple data items on the right under one heading on the left
func (w *Writer) AddStringsAsValue(t string, data []string) {
	maxLen := screenWidth()

	vals := make([]string, len(data))
	copy(vals, data)
	sort.Strings(vals)

	for i, val := range vals {
		if utf8StringLen(val) > maxLen && maxLen > 20 {
			w := maxLen/2 - 10
			val = fmt.Sprintf("%v ... %v", val[0:w], val[len(val)-w:])
		}

		if i == 0 {
			w.AddRowf(t, val)
		} else {
			w.AddRowf("", val)
		}
	}
}

// AddMapStringsAsValue adds a row with title t and the data as value, over multiple lines and correctly justified
func (w *Writer) AddMapStringsAsValue(t string, data map[string]string) {
	maxLen := screenWidth()

	var list []string
	for k := range data {
		list = append(list, k)
	}
	sort.Strings(list)

	for i, k := range list {
		v := data[k]

		if utf8StringLen(data[k]) > maxLen && maxLen > 20 {
			w := maxLen/2 - 10
			v = fmt.Sprintf("%v ... %v", v[0:w], v[len(v)-w:])
		}

		if i == 0 {
			w.AddRowf(t, "%s: %s", k, v)
		} else {
			w.AddRowf("", "%s: %s", k, v)
		}
	}
}

// AddMapInts adds data with each key being a column title and value what follows the :. Optionally sorts by value
func (w *Writer) AddMapInts(data map[string]int, sortValues bool, reverse bool) {
	var list []string
	for k := range data {
		list = append(list, k)
	}

	if sortValues {
		sort.Slice(list, func(i, j int) bool {
			if reverse {
				return data[list[i]] > data[list[j]]
			} else {
				return data[list[i]] < data[list[j]]
			}
		})
	}

	for _, k := range list {
		w.AddRowf(k, F(data[k]))
	}
}

// AddMapStrings adds data with each key being a column title and value what follows the :
func (w *Writer) AddMapStrings(data map[string]string) {
	maxLen := screenWidth()

	var list []string
	for k := range data {
		list = append(list, k)
	}
	sort.Strings(list)

	for _, k := range list {
		v := data[k]

		if utf8StringLen(data[k]) > maxLen && maxLen > 20 {
			w := maxLen/2 - 10
			v = fmt.Sprintf("%v ... %v", v[0:w], v[len(v)-w:])
		}

		w.AddRow(k, v)
	}
}

// Indent results in all following text to be indented this many spaces. When called it sets that value, zero resets to no indent
func (w *Writer) Indent(width int) {
	w.rows = append(w.rows, &columnRow{kind: kindIndent, values: []any{strings.Repeat(" ", width)}})
}

func (w *Writer) IsTerminal(o io.Writer) bool {
	fh, ok := any(o).(*os.File)
	if !ok {
		return false
	}

	return isatty.IsTerminal(fh.Fd())
}

func (w *Writer) maybeAddColon(o io.Writer, v string, colorize bool) string {
	if strings.HasSuffix(v, ":") {
		return v
	}

	c := ":"

	if colorize && w.IsTerminal(o) {
		color, ok := colsStyles[w.colorScheme]
		if ok {
			c = color.Sprint(":")
		}
	}

	return v + c
}

func utf8StringLen(s string) int {
	c := 0
	for range s {
		c++
	}

	return c
}

func F(v any) string {
	switch x := v.(type) {
	case []string:
		return strings.Join(x, ", ")
	case time.Duration:
		return HumanizeDuration(x)
	case time.Time:
		return x.Local().Format("2006-01-02 15:04:05")
	case bool:
		return fmt.Sprintf("%t", x)
	case uint:
		return humanize.Comma(int64(x))
	case uint32:
		return humanize.Comma(int64(x))
	case uint16:
		return humanize.Comma(int64(x))
	case uint64:
		return humanize.Comma(int64(x))
	case int:
		return humanize.Comma(int64(x))
	case int32:
		return humanize.Comma(int64(x))
	case int64:
		return humanize.Comma(x)
	case float32:
		return humanize.CommafWithDigits(float64(x), 3)
	case float64:
		return humanize.CommafWithDigits(x, 3)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func HumanizeDuration(d time.Duration) string {
	if d < time.Millisecond {
		return d.Round(time.Microsecond).String()
	}

	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}

	if d == math.MaxInt64 {
		return "never"
	}

	tsecs := d / time.Second
	tmins := tsecs / 60
	thrs := tmins / 60
	tdays := thrs / 24
	tyrs := tdays / 365

	if tyrs > 0 {
		return fmt.Sprintf("%dy%dd%dh%dm%ds", tyrs, tdays%365, thrs%24, tmins%60, tsecs%60)
	}

	if tdays > 0 {
		return fmt.Sprintf("%dd%dh%dm%ds", tdays, thrs%24, tmins%60, tsecs%60)
	}

	if thrs > 0 {
		return fmt.Sprintf("%dh%dm%ds", thrs, tmins%60, tsecs%60)
	}

	if tmins > 0 {
		return fmt.Sprintf("%dm%ds", tmins, tsecs%60)
	}

	return fmt.Sprintf("%.2fs", d.Seconds())
}

// calculates screen width
//
// if it cant figure out the width, assume 80
// if the width is too small, set it to minWidth and just live with the overflow
//
// this ensures a reasonable progress size, ideally we should switch over
// to a spinner for < minWidth rather than cause overflows, but thats for later.
func screenWidth() int {
	w, _, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 80
	}

	minWidth := 10

	if w-30 <= minWidth {
		return minWidth
	} else {
		return w - 30
	}
}
