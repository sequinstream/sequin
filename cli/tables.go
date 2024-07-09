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

package cli

import (
	"fmt"
	"sort"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

type tbl struct {
	writer table.Writer
}

var styles = map[string]table.Style{
	"":        table.StyleRounded,
	"rounded": table.StyleRounded,
	"double":  table.StyleDouble,
	"yellow":  coloredBorderStyle(text.FgYellow),
	"blue":    coloredBorderStyle(text.FgBlue),
	"cyan":    coloredBorderStyle(text.FgCyan),
	"green":   coloredBorderStyle(text.FgGreen),
	"magenta": coloredBorderStyle(text.FgMagenta),
	"red":     coloredBorderStyle(text.FgRed),
}

func coloredBorderStyle(c text.Color) table.Style {
	s := table.StyleRounded
	s.Color.Border = text.Colors{c}
	s.Color.Separator = text.Colors{c}
	s.Format.Footer = text.FormatDefault

	return s
}

// ValidStyles are valid color styles this package supports
func ValidStyles() []string {
	var res []string

	for k := range styles {
		if k == "" {
			continue
		}

		res = append(res, k)
	}

	sort.Strings(res)

	return res
}

func (t *tbl) AddHeaders(items ...any) {
	t.writer.AppendHeader(items)
}

func (t *tbl) AddFooter(items ...any) {
	t.writer.AppendFooter(items)
}

func (t *tbl) AddSeparator() {
	t.writer.AppendSeparator()
}

func (t *tbl) AddRow(items ...any) {
	t.writer.AppendRow(items)
}

func (t *tbl) Render() string {
	return fmt.Sprintln(t.writer.Render())
}
