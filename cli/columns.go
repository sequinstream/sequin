// Copyright 2024 The NATS Authors
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
	"sequin-cli/columns"

	"github.com/dustin/go-humanize"
)

func newColumns(heading string, a ...any) *columns.Writer {
	w := columns.New(heading, a...)
	// w.SetColorScheme(opts().Config.ColorScheme())
	w.SetHeading(heading, a...)

	return w
}

func fiBytes(v uint64) string {
	return humanize.IBytes(v)
}

func f(v any) string {
	return columns.F(v)
}
