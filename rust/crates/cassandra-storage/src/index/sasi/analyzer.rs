// Licensed under Apache License, Version 2.0.

//! SASI text analyzers for tokenization and normalization.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer`
//! - `org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer`
//! - `org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer`

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const ANALYZER_CLASS: &str = "analyzer_class";
const CASE_SENSITIVE: &str = "case_sensitive";
const NORMALIZE_LOWERCASE: &str = "normalize_lowercase";
const NORMALIZE_UPPERCASE: &str = "normalize_uppercase";
const TOKENIZATION_NORMALIZE_LOWERCASE: &str = "tokenization_normalize_lowercase";
const TOKENIZATION_NORMALIZE_UPPERCASE: &str = "tokenization_normalize_uppercase";

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
    /// Select an analyzer type from Java-compatible SASI options.
    pub fn from_options(options: &HashMap<String, String>) -> Self {
        options
            .get(ANALYZER_CLASS)
            .and_then(|name| {
                let normalized = name.trim().replace(['-', '_'], "").to_ascii_lowercase();
                match normalized.as_str() {
                    "standard"
                    | "standardanalyzer"
                    | "org.apache.cassandra.index.sasi.analyzer.standardanalyzer" => {
                        Some(AnalyzerType::Standard)
                    }
                    "non_tokenizing"
                    | "nontokenizing"
                    | "nontokenizinganalyzer"
                    | "org.apache.cassandra.index.sasi.analyzer.nontokenizinganalyzer" => {
                        Some(AnalyzerType::NonTokenizing)
                    }
                    _ => None,
                }
            })
            .unwrap_or_default()
    }

    /// Create the analyzer instance for this type.
    pub fn create(&self) -> Box<dyn Analyzer> {
        self.create_with_options(&HashMap::new())
    }

    /// Create the analyzer instance with Java-compatible SASI options.
    pub fn create_with_options(&self, options: &HashMap<String, String>) -> Box<dyn Analyzer> {
        match self {
            AnalyzerType::Standard => Box::new(StandardAnalyzer {
                transform: standard_transform(options),
            }),
            AnalyzerType::NonTokenizing => Box::new(NonTokenizingAnalyzer {
                transform: non_tokenizing_transform(options),
            }),
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
pub struct StandardAnalyzer {
    transform: CaseTransform,
}

impl Analyzer for StandardAnalyzer {
    fn analyze(&self, input: &str) -> Vec<String> {
        input
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(|s| self.transform.apply(s))
            .collect()
    }
}

/// Whole-value analyzer with optional lowercasing (no tokenization).
#[derive(Debug)]
pub struct NonTokenizingAnalyzer {
    transform: CaseTransform,
}

impl Analyzer for NonTokenizingAnalyzer {
    fn analyze(&self, input: &str) -> Vec<String> {
        if input.is_empty() {
            return Vec::new();
        }
        vec![self.transform.apply(input)]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaseTransform {
    Preserve,
    Lower,
    Upper,
}

impl CaseTransform {
    fn apply(self, input: &str) -> String {
        match self {
            Self::Preserve => input.to_string(),
            Self::Lower => input.to_lowercase(),
            Self::Upper => input.to_uppercase(),
        }
    }
}

fn standard_transform(options: &HashMap<String, String>) -> CaseTransform {
    if parse_bool_option(options, TOKENIZATION_NORMALIZE_UPPERCASE).unwrap_or(false) {
        CaseTransform::Upper
    } else if parse_bool_option(options, TOKENIZATION_NORMALIZE_LOWERCASE).unwrap_or(true) {
        CaseTransform::Lower
    } else {
        CaseTransform::Lower
    }
}

fn non_tokenizing_transform(options: &HashMap<String, String>) -> CaseTransform {
    if parse_bool_option(options, NORMALIZE_UPPERCASE).unwrap_or(false) {
        CaseTransform::Upper
    } else if parse_bool_option(options, NORMALIZE_LOWERCASE).unwrap_or(false)
        || !parse_bool_option(options, CASE_SENSITIVE).unwrap_or(true)
    {
        CaseTransform::Lower
    } else {
        CaseTransform::Preserve
    }
}

fn parse_bool_option(options: &HashMap<String, String>, key: &str) -> Option<bool> {
    options
        .get(key)
        .map(|value| value.eq_ignore_ascii_case("true"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_tokenizes_on_word_boundaries() {
        let a = StandardAnalyzer {
            transform: CaseTransform::Lower,
        };
        let tokens = a.analyze("Hello, World! Foo-Bar");
        assert_eq!(tokens, vec!["hello", "world", "foo", "bar"]);
    }

    #[test]
    fn standard_lowercases() {
        let a = StandardAnalyzer {
            transform: CaseTransform::Lower,
        };
        let tokens = a.analyze("CamelCase");
        assert_eq!(tokens, vec!["camelcase"]);
    }

    #[test]
    fn standard_empty_input() {
        let a = StandardAnalyzer {
            transform: CaseTransform::Lower,
        };
        assert!(a.analyze("").is_empty());
    }

    #[test]
    fn non_tokenizing_whole_value() {
        let a = NonTokenizingAnalyzer {
            transform: CaseTransform::Preserve,
        };
        let tokens = a.analyze("Hello World");
        assert_eq!(tokens, vec!["Hello World"]);
    }

    #[test]
    fn non_tokenizing_with_lowercase() {
        let a = NonTokenizingAnalyzer {
            transform: CaseTransform::Lower,
        };
        let tokens = a.analyze("Hello World");
        assert_eq!(tokens, vec!["hello world"]);
    }

    #[test]
    fn non_tokenizing_with_uppercase() {
        let a = NonTokenizingAnalyzer {
            transform: CaseTransform::Upper,
        };
        let tokens = a.analyze("Hello World");
        assert_eq!(tokens, vec!["HELLO WORLD"]);
    }

    #[test]
    fn non_tokenizing_empty() {
        let a = NonTokenizingAnalyzer {
            transform: CaseTransform::Lower,
        };
        assert!(a.analyze("").is_empty());
    }

    #[test]
    fn analyzer_type_factory() {
        let std_a = AnalyzerType::Standard.create();
        assert_eq!(std_a.analyze("Hello World"), vec!["hello", "world"]);

        let nt_a = AnalyzerType::NonTokenizing.create();
        assert_eq!(nt_a.analyze("Hello World"), vec!["Hello World"]);
    }

    #[test]
    fn analyzer_type_accepts_java_class_names() {
        let mut options = HashMap::new();
        options.insert(
            ANALYZER_CLASS.to_string(),
            "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer".to_string(),
        );
        assert_eq!(
            AnalyzerType::from_options(&options),
            AnalyzerType::NonTokenizing
        );

        options.insert(
            ANALYZER_CLASS.to_string(),
            "org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer".to_string(),
        );
        assert_eq!(AnalyzerType::from_options(&options), AnalyzerType::Standard);
    }

    #[test]
    fn non_tokenizing_options_drive_case_transform() {
        let mut options = HashMap::new();
        options.insert(CASE_SENSITIVE.to_string(), "false".to_string());
        let analyzer = AnalyzerType::NonTokenizing.create_with_options(&options);
        assert_eq!(analyzer.analyze("Hello World"), vec!["hello world"]);

        options.clear();
        options.insert(NORMALIZE_UPPERCASE.to_string(), "true".to_string());
        let analyzer = AnalyzerType::NonTokenizing.create_with_options(&options);
        assert_eq!(analyzer.analyze("Hello World"), vec!["HELLO WORLD"]);
    }
}
