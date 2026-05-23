// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Nodetool-style table formatting.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.nodetool.formatter.TableBuilder`
//! - `org.apache.cassandra.tools.nodetool.formatter.TableFormatter`

/// Column alignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Alignment {
    Left,
    Right,
}

/// A table column definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableColumn {
    heading: String,
    alignment: Alignment,
    min_width: usize,
}

impl TableColumn {
    pub fn left(heading: impl Into<String>) -> Self {
        Self {
            heading: heading.into(),
            alignment: Alignment::Left,
            min_width: 0,
        }
    }

    pub fn right(heading: impl Into<String>) -> Self {
        Self {
            heading: heading.into(),
            alignment: Alignment::Right,
            min_width: 0,
        }
    }

    pub fn min_width(mut self, width: usize) -> Self {
        self.min_width = width;
        self
    }
}

/// Fixed-width table formatter with nodetool-compatible plain-text output.
#[derive(Debug, Clone)]
pub struct TableFormatter {
    columns: Vec<TableColumn>,
    rows: Vec<Vec<String>>,
    padding: usize,
}

impl TableFormatter {
    pub fn new(columns: Vec<TableColumn>) -> Self {
        assert!(!columns.is_empty(), "table must have at least one column");
        Self {
            columns,
            rows: Vec::new(),
            padding: 1,
        }
    }

    pub fn add_row<I, S>(&mut self, values: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let row: Vec<String> = values.into_iter().map(Into::into).collect();
        assert_eq!(
            row.len(),
            self.columns.len(),
            "row column count must match table columns"
        );
        self.rows.push(row);
    }

    pub fn render(&self) -> String {
        let widths = self.column_widths();
        let mut lines = Vec::with_capacity(self.rows.len() + 1);

        let headings = self
            .columns
            .iter()
            .map(|column| column.heading.as_str())
            .collect::<Vec<_>>();
        lines.push(self.render_cells(&headings, &widths));

        for row in &self.rows {
            let cells = row.iter().map(String::as_str).collect::<Vec<_>>();
            lines.push(self.render_cells(&cells, &widths));
        }

        lines.join("\n")
    }

    fn column_widths(&self) -> Vec<usize> {
        let mut widths = self
            .columns
            .iter()
            .map(|column| column.min_width.max(display_width(&column.heading)))
            .collect::<Vec<_>>();

        for row in &self.rows {
            for (idx, value) in row.iter().enumerate() {
                widths[idx] = widths[idx].max(display_width(value));
            }
        }

        widths
    }

    fn render_cells(&self, cells: &[&str], widths: &[usize]) -> String {
        let gap = " ".repeat(self.padding);
        cells
            .iter()
            .enumerate()
            .map(|(idx, cell)| {
                let alignment = self.columns[idx].alignment;
                pad_cell(cell, widths[idx], alignment)
            })
            .collect::<Vec<_>>()
            .join(&gap)
    }
}

fn pad_cell(value: &str, width: usize, alignment: Alignment) -> String {
    let len = display_width(value);
    if len >= width {
        return value.to_string();
    }
    let pad = " ".repeat(width - len);
    match alignment {
        Alignment::Left => format!("{value}{pad}"),
        Alignment::Right => format!("{pad}{value}"),
    }
}

fn display_width(value: &str) -> usize {
    value.chars().count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_left_and_right_aligned_columns() {
        let mut table = TableFormatter::new(vec![
            TableColumn::left("Name").min_width(5),
            TableColumn::right("Count").min_width(5),
        ]);
        table.add_row(["Read", "7"]);
        table.add_row(["Mutation", "123"]);

        assert_eq!(
            table.render(),
            "Name     Count\nRead         7\nMutation   123"
        );
    }

    #[test]
    fn grows_columns_to_fit_values() {
        let mut table = TableFormatter::new(vec![TableColumn::left("A"), TableColumn::left("B")]);
        table.add_row(["short", "much-longer"]);

        assert_eq!(table.render(), "A     B          \nshort much-longer");
    }

    #[test]
    #[should_panic(expected = "row column count must match table columns")]
    fn rejects_wrong_width_rows() {
        let mut table = TableFormatter::new(vec![TableColumn::left("A")]);
        table.add_row(["a", "b"]);
    }
}
