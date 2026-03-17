// Licensed under Apache License, Version 2.0.

//! SASI text analyzers for tokenization and normalization.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer`
//! - `org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer`
//! - `org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer`

use serde::{Deserialize, Serialize};

/// An analyzer tokenizes and normalizes input text for indexing.
pub trait Analyzer: Send + Sync + std::fmt::Debug {
    /// Tokenize and normalize the input into index terms.
    fn analyze(&self, input: &str) -> Vec<String>;
}

/// Analyzer type selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnalyzerType {
    Standard,
    NonTokenizing,
}

impl AnalyzerType {
    /// Create the analyzer instance for this type.
    pub fn create(&self) -> Box<dyn Analyzer> {
        match self {
            AnalyzerType::Standard => Box::new(StandardAnalyzer),
            AnalyzerType::NonTokenizing => Box::new(NonTokenizingAnalyzer { lowercase: true }),
        }
    }
}

impl Default for AnalyzerType {
    fn default() -> Self {
        AnalyzerType::Standard
    }
}

/// Word-boundary tokenizer with lowercasing.
///
/// Splits on non-alphanumeric characters and lowercases each token.
#[derive(Debug)]
pub struct StandardAnalyzer;

impl Analyzer for StandardAnalyzer {
    fn analyze(&self, input: &str) -> Vec<String> {
        input
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_lowercase())
            .collect()
    }
}

/// Whole-value analyzer with optional lowercasing (no tokenization).
#[derive(Debug)]
pub struct NonTokenizingAnalyzer {
    pub lowercase: bool,
}

impl Analyzer for NonTokenizingAnalyzer {
    fn analyze(&self, input: &str) -> Vec<String> {
        if input.is_empty() {
            return Vec::new();
        }
        if self.lowercase {
            vec![input.to_lowercase()]
        } else {
            vec![input.to_string()]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_tokenizes_on_word_boundaries() {
        let a = StandardAnalyzer;
        let tokens = a.analyze("Hello, World! Foo-Bar");
        assert_eq!(tokens, vec!["hello", "world", "foo", "bar"]);
    }

    #[test]
    fn standard_lowercases() {
        let a = StandardAnalyzer;
        let tokens = a.analyze("CamelCase");
        assert_eq!(tokens, vec!["camelcase"]);
    }

    #[test]
    fn standard_empty_input() {
        let a = StandardAnalyzer;
        assert!(a.analyze("").is_empty());
    }

    #[test]
    fn non_tokenizing_whole_value() {
        let a = NonTokenizingAnalyzer { lowercase: false };
        let tokens = a.analyze("Hello World");
        assert_eq!(tokens, vec!["Hello World"]);
    }

    #[test]
    fn non_tokenizing_with_lowercase() {
        let a = NonTokenizingAnalyzer { lowercase: true };
        let tokens = a.analyze("Hello World");
        assert_eq!(tokens, vec!["hello world"]);
    }

    #[test]
    fn non_tokenizing_empty() {
        let a = NonTokenizingAnalyzer { lowercase: true };
        assert!(a.analyze("").is_empty());
    }

    #[test]
    fn analyzer_type_factory() {
        let std_a = AnalyzerType::Standard.create();
        assert_eq!(std_a.analyze("Hello World"), vec!["hello", "world"]);

        let nt_a = AnalyzerType::NonTokenizing.create();
        assert_eq!(nt_a.analyze("Hello World"), vec!["hello world"]);
    }
}
