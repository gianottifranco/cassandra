// Licensed under Apache License, Version 2.0.

//! SAI text analyzers.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer`
//! - `org.apache.cassandra.index.sai.analyzer.NonTokenizingAnalyzer`
//! - `org.apache.cassandra.index.sai.analyzer.NonTokenizingOptions`

use std::collections::HashMap;

/// Analyzer option: select the analyzer implementation.
pub const ANALYZER_CLASS: &str = "analyzer_class";
/// Analyzer option: shorter alias accepted by Rust configuration.
pub const ANALYZER: &str = "analyzer";
/// Analyzer option: preserve input case when true.
pub const CASE_SENSITIVE: &str = "case_sensitive";
/// Analyzer option: normalize Unicode text.
pub const NORMALIZE: &str = "normalize";
/// Analyzer option: fold common non-ASCII characters to ASCII.
pub const ASCII: &str = "ascii";

const STANDARD_ANALYZER_NAMES: &[&str] = &[
    "standard",
    "standardanalyzer",
    "org.apache.cassandra.index.sai.analyzer.standardanalyzer",
    "org.apache.lucene.analysis.standard.standardanalyzer",
];

const NON_TOKENIZING_ANALYZER_NAMES: &[&str] = &[
    "non_tokenizing",
    "nontokenizing",
    "nontokenizinganalyzer",
    "org.apache.cassandra.index.sai.analyzer.nontokenizinganalyzer",
];

/// An SAI analyzer transforms an indexed/query term into one or more terms.
pub trait SaiAnalyzer: Send + Sync + std::fmt::Debug {
    fn analyze(&self, input: &str) -> Vec<String>;
    fn transform_value(&self) -> bool;
}

/// Analyzer selected from SAI index options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnalyzerInstance {
    NonTokenizing(NonTokenizingAnalyzer),
    Standard(StandardAnalyzer),
}

impl AnalyzerInstance {
    pub fn non_tokenizing_options(&self) -> Option<NonTokenizingOptions> {
        match self {
            Self::NonTokenizing(analyzer) => Some(analyzer.options()),
            Self::Standard(_) => None,
        }
    }
}

impl SaiAnalyzer for AnalyzerInstance {
    fn analyze(&self, input: &str) -> Vec<String> {
        match self {
            Self::NonTokenizing(analyzer) => analyzer.analyze(input),
            Self::Standard(analyzer) => analyzer.analyze(input),
        }
    }

    fn transform_value(&self) -> bool {
        match self {
            Self::NonTokenizing(analyzer) => analyzer.transform_value(),
            Self::Standard(analyzer) => analyzer.transform_value(),
        }
    }
}

/// Java-compatible SAI non-tokenizing analyzer options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonTokenizingOptions {
    pub case_sensitive: bool,
    pub normalize: bool,
    pub ascii: bool,
}

impl NonTokenizingOptions {
    pub fn from_map(options: &HashMap<String, String>) -> Result<Self, String> {
        let mut parsed = Self::default();
        for (key, value) in options {
            match key.as_str() {
                CASE_SENSITIVE => parsed.case_sensitive = parse_bool_option(key, value)?,
                NORMALIZE => parsed.normalize = parse_bool_option(key, value)?,
                ASCII => parsed.ascii = parse_bool_option(key, value)?,
                _ => {}
            }
        }
        Ok(parsed)
    }

    pub fn has_option(option: &str) -> bool {
        matches!(option, CASE_SENSITIVE | NORMALIZE | ASCII)
    }

    pub fn analyzer_options(options: &HashMap<String, String>) -> HashMap<String, String> {
        options
            .iter()
            .filter(|(key, _)| Self::has_option(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

impl Default for NonTokenizingOptions {
    fn default() -> Self {
        Self {
            case_sensitive: true,
            normalize: false,
            ascii: false,
        }
    }
}

/// Whole-value SAI analyzer with Java's non-tokenizing option filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonTokenizingAnalyzer {
    options: NonTokenizingOptions,
}

impl NonTokenizingAnalyzer {
    pub fn new(options: NonTokenizingOptions) -> Self {
        Self { options }
    }

    pub fn options(&self) -> NonTokenizingOptions {
        self.options
    }
}

impl SaiAnalyzer for NonTokenizingAnalyzer {
    fn analyze(&self, input: &str) -> Vec<String> {
        if input.is_empty() {
            return Vec::new();
        }

        vec![apply_filters(input, self.options)]
    }

    fn transform_value(&self) -> bool {
        !self.options.case_sensitive || self.options.normalize || self.options.ascii
    }
}

/// Tokenizing SAI analyzer for string columns.
///
/// It follows the same filter order as the non-tokenizing analyzer, then emits
/// contiguous alphanumeric runs as index terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandardAnalyzer {
    options: NonTokenizingOptions,
}

impl StandardAnalyzer {
    pub fn new(options: NonTokenizingOptions) -> Self {
        Self { options }
    }

    pub fn options(&self) -> NonTokenizingOptions {
        self.options
    }
}

impl SaiAnalyzer for StandardAnalyzer {
    fn analyze(&self, input: &str) -> Vec<String> {
        if input.is_empty() {
            return Vec::new();
        }

        apply_filters(input, self.options)
            .split(|ch: char| !ch.is_alphanumeric())
            .filter(|term| !term.is_empty())
            .map(str::to_string)
            .collect()
    }

    fn transform_value(&self) -> bool {
        true
    }
}

/// Create an analyzer from SAI options. Returns `None` when no SAI analyzer
/// options are present, matching Java's `AbstractAnalyzer.fromOptions`.
pub fn analyzer_from_options(
    options: &HashMap<String, String>,
) -> Result<Option<AnalyzerInstance>, String> {
    let analyzer_options = NonTokenizingOptions::analyzer_options(options);
    let filters = NonTokenizingOptions::from_map(&analyzer_options)?;

    match selected_analyzer(options)? {
        Some(SelectedAnalyzer::Standard) => Ok(Some(AnalyzerInstance::Standard(
            StandardAnalyzer::new(filters),
        ))),
        Some(SelectedAnalyzer::NonTokenizing) => Ok(Some(AnalyzerInstance::NonTokenizing(
            NonTokenizingAnalyzer::new(filters),
        ))),
        None if analyzer_options.is_empty() => Ok(None),
        None => Ok(Some(AnalyzerInstance::NonTokenizing(
            NonTokenizingAnalyzer::new(filters),
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectedAnalyzer {
    Standard,
    NonTokenizing,
}

fn selected_analyzer(
    options: &HashMap<String, String>,
) -> Result<Option<SelectedAnalyzer>, String> {
    let Some(raw) = options
        .get(ANALYZER_CLASS)
        .or_else(|| options.get(ANALYZER))
    else {
        return Ok(None);
    };
    let normalized = raw.trim().replace(['-', '_'], "").to_ascii_lowercase();

    if STANDARD_ANALYZER_NAMES.contains(&normalized.as_str()) {
        Ok(Some(SelectedAnalyzer::Standard))
    } else if NON_TOKENIZING_ANALYZER_NAMES.contains(&normalized.as_str()) {
        Ok(Some(SelectedAnalyzer::NonTokenizing))
    } else {
        Err(format!("Unsupported SAI analyzer: {raw}"))
    }
}

fn apply_filters(input: &str, options: NonTokenizingOptions) -> String {
    let mut value = input.to_string();
    if !options.case_sensitive {
        value = value.to_lowercase();
    }
    if options.normalize {
        value = normalize_nfc_subset(&value);
    }
    if options.ascii {
        value = fold_ascii_subset(&value);
    }
    value
}

fn parse_bool_option(key: &str, value: &str) -> Result<bool, String> {
    if value.is_empty() {
        return Err(format!("Empty value for boolean option '{key}'"));
    }
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("Illegal value for boolean option '{key}': {value}")),
    }
}

fn normalize_nfc_subset(input: &str) -> String {
    input
        .replace("e\u{301}", "é")
        .replace("E\u{301}", "É")
        .replace("a\u{301}", "á")
        .replace("A\u{301}", "Á")
        .replace("o\u{308}", "ö")
        .replace("O\u{308}", "Ö")
}

fn fold_ascii_subset(input: &str) -> String {
    input
        .chars()
        .map(|ch| match ch {
            'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' | 'ā' => 'a',
            'Á' | 'À' | 'Â' | 'Ä' | 'Ã' | 'Å' | 'Ā' => 'A',
            'ç' => 'c',
            'Ç' => 'C',
            'é' | 'è' | 'ê' | 'ë' | 'ē' => 'e',
            'É' | 'È' | 'Ê' | 'Ë' | 'Ē' => 'E',
            'í' | 'ì' | 'î' | 'ï' | 'ī' => 'i',
            'Í' | 'Ì' | 'Î' | 'Ï' | 'Ī' => 'I',
            'ñ' => 'n',
            'Ñ' => 'N',
            'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'ō' => 'o',
            'Ó' | 'Ò' | 'Ô' | 'Ö' | 'Õ' | 'Ō' => 'O',
            'ú' | 'ù' | 'û' | 'ü' | 'ū' => 'u',
            'Ú' | 'Ù' | 'Û' | 'Ü' | 'Ū' => 'U',
            'ý' | 'ÿ' => 'y',
            'Ý' | 'Ÿ' => 'Y',
            'ß' => 's',
            other => other,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_do_not_transform() {
        let analyzer = NonTokenizingAnalyzer::new(NonTokenizingOptions::default());
        assert!(!analyzer.transform_value());
        assert_eq!(analyzer.analyze("Hello World"), vec!["Hello World"]);
    }

    #[test]
    fn lowercases_when_case_insensitive() {
        let analyzer = NonTokenizingAnalyzer::new(NonTokenizingOptions {
            case_sensitive: false,
            normalize: false,
            ascii: false,
        });
        assert!(analyzer.transform_value());
        assert_eq!(analyzer.analyze("Hello World"), vec!["hello world"]);
    }

    #[test]
    fn parses_and_filters_options() {
        let mut options = HashMap::new();
        options.insert(CASE_SENSITIVE.to_string(), "false".to_string());
        options.insert(NORMALIZE.to_string(), "true".to_string());
        options.insert("irrelevant".to_string(), "x".to_string());
        let analyzer_options = NonTokenizingOptions::analyzer_options(&options);
        assert_eq!(analyzer_options.len(), 2);
        let parsed = NonTokenizingOptions::from_map(&analyzer_options).unwrap();
        assert!(!parsed.case_sensitive);
        assert!(parsed.normalize);
    }

    #[test]
    fn rejects_invalid_boolean_option() {
        let mut options = HashMap::new();
        options.insert(ASCII.to_string(), "maybe".to_string());
        assert!(NonTokenizingOptions::from_map(&options).is_err());
    }

    #[test]
    fn folds_ascii_subset_after_normalization() {
        let analyzer = NonTokenizingAnalyzer::new(NonTokenizingOptions {
            case_sensitive: false,
            normalize: true,
            ascii: true,
        });
        assert_eq!(analyzer.analyze("Cafe\u{301} ÉLAN"), vec!["cafe elan"]);
    }

    #[test]
    fn factory_is_none_without_analyzer_options() {
        assert!(analyzer_from_options(&HashMap::new()).unwrap().is_none());
    }

    #[test]
    fn standard_analyzer_tokenizes_and_filters() {
        let analyzer = StandardAnalyzer::new(NonTokenizingOptions {
            case_sensitive: false,
            normalize: true,
            ascii: true,
        });
        assert!(analyzer.transform_value());
        assert_eq!(
            analyzer.analyze("Cafe\u{301}, ÉLAN-42 foo_bar"),
            vec!["cafe", "elan", "42", "foo", "bar"]
        );
    }

    #[test]
    fn factory_selects_standard_analyzer() {
        let mut options = HashMap::new();
        options.insert(ANALYZER_CLASS.to_string(), "standard".to_string());
        options.insert(CASE_SENSITIVE.to_string(), "false".to_string());

        let analyzer = analyzer_from_options(&options).unwrap().unwrap();
        assert_eq!(analyzer.analyze("Hello, World"), vec!["hello", "world"]);
        assert!(matches!(analyzer, AnalyzerInstance::Standard(_)));
    }

    #[test]
    fn factory_rejects_unknown_analyzer() {
        let mut options = HashMap::new();
        options.insert(ANALYZER.to_string(), "unknown".to_string());
        assert!(analyzer_from_options(&options).is_err());
    }
}
