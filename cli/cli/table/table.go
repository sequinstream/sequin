package table

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

type Column struct {
	Name      string
	MinWidth  int
	ValueFunc func(row interface{}) string
}

type Table struct {
	Columns        []Column
	Rows           []interface{}
	SelectedIndex  int
	ActionColumn   *Column
	HeaderStyle    lipgloss.Style
	RowStyle       lipgloss.Style
	SelectedStyle  lipgloss.Style
	ActionStyle    lipgloss.Style
	TotalWidth     int
	ActionOnSelect bool
}

func NewTable(columns []Column, rows []interface{}, totalWidth int) *Table {
	return &Table{
		Columns:    columns,
		Rows:       rows,
		TotalWidth: totalWidth,
		HeaderStyle: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("0")).
			Background(lipgloss.Color("252")),
		RowStyle:      lipgloss.NewStyle(),
		SelectedStyle: lipgloss.NewStyle().Background(lipgloss.Color("57")).Foreground(lipgloss.Color("255")),
		ActionStyle:   lipgloss.NewStyle(),
	}
}

func (t *Table) SetActionColumn(name string, valueFunc func(row interface{}) string) {
	t.ActionColumn = &Column{
		Name:      name,
		ValueFunc: valueFunc,
	}
}

func (t *Table) calculateColumnWidths() []int {
	widths := make([]int, len(t.Columns))

	// Calculate initial widths based on content and MinWidth
	for i, col := range t.Columns {
		width := len(col.Name)
		for _, row := range t.Rows {
			valueWidth := len(col.ValueFunc(row))
			if valueWidth > width {
				width = valueWidth
			}
		}
		if width < col.MinWidth {
			width = col.MinWidth
		}
		widths[i] = width
	}

	// Calculate space needed for action column
	actionColumnWidth := 0
	if t.ActionColumn != nil {
		actionColumnWidth = len(t.ActionColumn.Name)
		for _, row := range t.Rows {
			valueWidth := len(t.ActionColumn.ValueFunc(row))
			if valueWidth > actionColumnWidth {
				actionColumnWidth = valueWidth
			}
		}
	}

	// Calculate total width used and available space
	totalUsedWidth := sum(widths) + len(widths) - 1        // Add space for separators
	availableWidth := t.TotalWidth - actionColumnWidth - 1 // Reserve space for action column and its separator

	// Adjust widths if necessary
	if totalUsedWidth > availableWidth {
		// Reduce widths proportionally, but respect MinWidth
		excessWidth := totalUsedWidth - availableWidth
		totalAdjustableWidth := 0
		for i, width := range widths {
			if width > t.Columns[i].MinWidth {
				totalAdjustableWidth += width - t.Columns[i].MinWidth
			}
		}

		for i := range widths {
			if widths[i] > t.Columns[i].MinWidth {
				reduction := int(float64(excessWidth) * float64(widths[i]-t.Columns[i].MinWidth) / float64(totalAdjustableWidth))
				widths[i] -= reduction
				if widths[i] < t.Columns[i].MinWidth {
					widths[i] = t.Columns[i].MinWidth
				}
				excessWidth -= reduction
			}
		}

		// If there's still excess width, take it from the largest column
		for excessWidth > 0 {
			largestIdx := 0
			for i := 1; i < len(widths); i++ {
				if widths[i] > widths[largestIdx] && widths[i] > t.Columns[i].MinWidth {
					largestIdx = i
				}
			}
			if widths[largestIdx] > t.Columns[largestIdx].MinWidth {
				widths[largestIdx]--
				excessWidth--
			} else {
				break // Can't reduce any further without violating MinWidth
			}
		}
	} else if totalUsedWidth < availableWidth {
		// Distribute extra space proportionally
		extraWidth := availableWidth - totalUsedWidth
		totalWeight := sum(widths)
		for i := range widths {
			proportion := float64(widths[i]) / float64(totalWeight)
			addition := int(float64(extraWidth) * proportion)
			widths[i] += addition
			extraWidth -= addition
		}
		// Distribute any remaining width due to rounding
		for i := 0; extraWidth > 0; i = (i + 1) % len(widths) {
			widths[i]++
			extraWidth--
		}
	}

	return widths
}

func (t *Table) Render() string {
	widths := t.calculateColumnWidths()
	var output strings.Builder

	// Calculate action column width
	actionColumnWidth := t.TotalWidth - sum(widths) - len(widths)
	if actionColumnWidth < 0 {
		actionColumnWidth = 0
	}

	// Render header
	headerCells := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		headerCells[i] = fmt.Sprintf("%-*s", widths[i], truncateString(col.Name, widths[i]))
	}
	if t.ActionColumn != nil {
		headerCells = append(headerCells, fmt.Sprintf("%-*s", actionColumnWidth, t.ActionColumn.Name))
	}
	header := strings.Join(headerCells, " ")
	output.WriteString(t.HeaderStyle.Render(header) + "\n")

	// Render rows
	for i, row := range t.Rows {
		cells := make([]string, len(t.Columns))
		for j, col := range t.Columns {
			if widths[j] > 0 {
				cells[j] = fmt.Sprintf("%-*s", widths[j], truncateString(col.ValueFunc(row), widths[j]))
			} else {
				cells[j] = ""
			}
		}

		style := t.RowStyle
		if i == t.SelectedIndex {
			style = t.SelectedStyle
			if t.ActionColumn != nil && t.ActionOnSelect {
				cells = append(cells, fmt.Sprintf("%-*s", actionColumnWidth, truncateString(t.ActionColumn.ValueFunc(row), actionColumnWidth)))
			} else if t.ActionColumn != nil && actionColumnWidth > 0 {
				cells = append(cells, strings.Repeat(" ", actionColumnWidth))
			}
		} else if t.ActionColumn != nil && actionColumnWidth > 0 {
			cells = append(cells, strings.Repeat(" ", actionColumnWidth))
		}

		line := strings.Join(cells, " ")
		output.WriteString(style.Render(line) + "\n")
	}

	return output.String()
}

// Helper function to truncate strings
func truncateString(s string, maxLength int) string {
	if maxLength <= 0 {
		return ""
	}
	if len(s) <= maxLength {
		return s
	}
	return s[:maxLength]
}

func sum(numbers []int) int {
	total := 0
	for _, num := range numbers {
		total += num
	}
	return total
}
