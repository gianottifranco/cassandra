// Licensed under Apache License, Version 2.0.

//! CQL lexer / tokenizer.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.CqlLexer` (ANTLR-generated)

use std::fmt;

/// A CQL token with its position in the input.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

/// Source position span.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // ── Keywords (case-insensitive) ──
    Keyword(Keyword),
    // ── Identifiers ──
    Ident(String),
    QuotedIdent(String),
    // ── Literals ──
    StringLiteral(String),
    IntegerLiteral(i64),
    FloatLiteral(f64),
    BlobLiteral(Vec<u8>),
    UuidLiteral(String),
    BooleanLiteral(bool),
    NullLiteral,
    // ── Bind markers ──
    QuestionMark,
    NamedBind(String),
    // ── Punctuation / operators ──
    LParen,
    RParen,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Comma,
    Semicolon,
    Dot,
    Star,
    Plus,
    Minus,
    Slash,
    Percent,
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    Colon,
    // ── Special ──
    Eof,
}

impl fmt::Display for TokenKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenKind::Keyword(k) => write!(f, "{:?}", k),
            TokenKind::Ident(s) => write!(f, "identifier '{}'", s),
            TokenKind::QuotedIdent(s) => write!(f, "\"{}\"", s),
            TokenKind::StringLiteral(s) => write!(f, "'{}'", s),
            TokenKind::IntegerLiteral(n) => write!(f, "{}", n),
            TokenKind::FloatLiteral(v) => write!(f, "{}", v),
            TokenKind::BlobLiteral(_) => write!(f, "blob"),
            TokenKind::UuidLiteral(s) => write!(f, "uuid '{}'", s),
            TokenKind::BooleanLiteral(b) => write!(f, "{}", b),
            TokenKind::NullLiteral => write!(f, "NULL"),
            TokenKind::QuestionMark => write!(f, "?"),
            TokenKind::NamedBind(s) => write!(f, ":{}", s),
            TokenKind::LParen => write!(f, "("),
            TokenKind::RParen => write!(f, ")"),
            TokenKind::LBrace => write!(f, "{{"),
            TokenKind::RBrace => write!(f, "}}"),
            TokenKind::LBracket => write!(f, "["),
            TokenKind::RBracket => write!(f, "]"),
            TokenKind::Comma => write!(f, ","),
            TokenKind::Semicolon => write!(f, ";"),
            TokenKind::Dot => write!(f, "."),
            TokenKind::Star => write!(f, "*"),
            TokenKind::Plus => write!(f, "+"),
            TokenKind::Minus => write!(f, "-"),
            TokenKind::Slash => write!(f, "/"),
            TokenKind::Percent => write!(f, "%"),
            TokenKind::Eq => write!(f, "="),
            TokenKind::Neq => write!(f, "!="),
            TokenKind::Lt => write!(f, "<"),
            TokenKind::Gt => write!(f, ">"),
            TokenKind::Lte => write!(f, "<="),
            TokenKind::Gte => write!(f, ">="),
            TokenKind::Colon => write!(f, ":"),
            TokenKind::Eof => write!(f, "EOF"),
        }
    }
}

/// CQL keywords (case-insensitive).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Keyword {
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    In,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Alter,
    Drop,
    Keyspace,
    Table,
    If,
    Exists,
    Primary,
    Key,
    With,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Allow,
    Filtering,
    Use,
    Truncate,
    Batch,
    Apply,
    Begin,
    Unlogged,
    Counter,
    Using,
    Timestamp,
    Ttl,
    Token,
    Contains,
    Distinct,
    Json,
    Null,
    True,
    False,
    Frozen,
    List,
    Map,
    Tuple,
    Clustering,
    Compact,
    Storage,
    Index,
    On,
    Type,
    Add,
    Rename,
    Column,
    Static,
    Materialized,
    View,
    As,
    Is,
    Like,
    Per,
    Partition,
    Group,
    Replication,
    DurableWrites,
    // Phase 12: DCL, UDF/UDA, triggers, long tail
    Role,
    Roles,
    Grant,
    Revoke,
    Permission,
    Permissions,
    Function,
    Aggregate,
    Trigger,
    Returns,
    Language,
    Called,
    Input,
    Sfunc,
    Stype,
    Finalfunc,
    Initcond,
    Custom,
    Replace,
    Login,
    Superuser,
    Password,
    Norecursive,
    Of,
    All,
    Cast,
    Vector,
    Masked,
}

impl Keyword {
    pub fn from_str_ci(s: &str) -> Option<Keyword> {
        match s.to_uppercase().as_str() {
            "SELECT" => Some(Keyword::Select),
            "FROM" => Some(Keyword::From),
            "WHERE" => Some(Keyword::Where),
            "AND" => Some(Keyword::And),
            "OR" => Some(Keyword::Or),
            "NOT" => Some(Keyword::Not),
            "IN" => Some(Keyword::In),
            "INSERT" => Some(Keyword::Insert),
            "INTO" => Some(Keyword::Into),
            "VALUES" => Some(Keyword::Values),
            "UPDATE" => Some(Keyword::Update),
            "SET" => Some(Keyword::Set),
            "DELETE" => Some(Keyword::Delete),
            "CREATE" => Some(Keyword::Create),
            "ALTER" => Some(Keyword::Alter),
            "DROP" => Some(Keyword::Drop),
            "KEYSPACE" => Some(Keyword::Keyspace),
            "TABLE" => Some(Keyword::Table),
            "IF" => Some(Keyword::If),
            "EXISTS" => Some(Keyword::Exists),
            "PRIMARY" => Some(Keyword::Primary),
            "KEY" => Some(Keyword::Key),
            "WITH" => Some(Keyword::With),
            "ORDER" => Some(Keyword::Order),
            "BY" => Some(Keyword::By),
            "ASC" => Some(Keyword::Asc),
            "DESC" => Some(Keyword::Desc),
            "LIMIT" => Some(Keyword::Limit),
            "ALLOW" => Some(Keyword::Allow),
            "FILTERING" => Some(Keyword::Filtering),
            "USE" => Some(Keyword::Use),
            "TRUNCATE" => Some(Keyword::Truncate),
            "BATCH" => Some(Keyword::Batch),
            "APPLY" => Some(Keyword::Apply),
            "BEGIN" => Some(Keyword::Begin),
            "UNLOGGED" => Some(Keyword::Unlogged),
            "COUNTER" => Some(Keyword::Counter),
            "USING" => Some(Keyword::Using),
            "TIMESTAMP" => Some(Keyword::Timestamp),
            "TTL" => Some(Keyword::Ttl),
            "TOKEN" => Some(Keyword::Token),
            "CONTAINS" => Some(Keyword::Contains),
            "DISTINCT" => Some(Keyword::Distinct),
            "JSON" => Some(Keyword::Json),
            "NULL" => Some(Keyword::Null),
            "TRUE" => Some(Keyword::True),
            "FALSE" => Some(Keyword::False),
            "FROZEN" => Some(Keyword::Frozen),
            "LIST" => Some(Keyword::List),
            "MAP" => Some(Keyword::Map),
            "SET" => Some(Keyword::Set),
            "TUPLE" => Some(Keyword::Tuple),
            "CLUSTERING" => Some(Keyword::Clustering),
            "COMPACT" => Some(Keyword::Compact),
            "STORAGE" => Some(Keyword::Storage),
            "INDEX" => Some(Keyword::Index),
            "ON" => Some(Keyword::On),
            "TYPE" => Some(Keyword::Type),
            "ADD" => Some(Keyword::Add),
            "RENAME" => Some(Keyword::Rename),
            "COLUMN" => Some(Keyword::Column),
            "STATIC" => Some(Keyword::Static),
            "MATERIALIZED" => Some(Keyword::Materialized),
            "VIEW" => Some(Keyword::View),
            "AS" => Some(Keyword::As),
            "IS" => Some(Keyword::Is),
            "LIKE" => Some(Keyword::Like),
            "PER" => Some(Keyword::Per),
            "PARTITION" => Some(Keyword::Partition),
            "GROUP" => Some(Keyword::Group),
            "REPLICATION" => Some(Keyword::Replication),
            "DURABLE_WRITES" => Some(Keyword::DurableWrites),
            // Phase 12: new keywords
            "ROLE" => Some(Keyword::Role),
            "ROLES" => Some(Keyword::Roles),
            "GRANT" => Some(Keyword::Grant),
            "REVOKE" => Some(Keyword::Revoke),
            "PERMISSION" => Some(Keyword::Permission),
            "PERMISSIONS" => Some(Keyword::Permissions),
            "FUNCTION" => Some(Keyword::Function),
            "AGGREGATE" => Some(Keyword::Aggregate),
            "TRIGGER" => Some(Keyword::Trigger),
            "RETURNS" => Some(Keyword::Returns),
            "LANGUAGE" => Some(Keyword::Language),
            "CALLED" | "CALLS" => Some(Keyword::Called),
            "SFUNC" => Some(Keyword::Sfunc),
            "STYPE" => Some(Keyword::Stype),
            "FINALFUNC" => Some(Keyword::Finalfunc),
            "INITCOND" => Some(Keyword::Initcond),
            "CUSTOM" => Some(Keyword::Custom),
            "REPLACE" => Some(Keyword::Replace),
            "LOGIN" => Some(Keyword::Login),
            "SUPERUSER" => Some(Keyword::Superuser),
            "NOSUPERUSER" => Some(Keyword::Superuser), // handled at parser level
            "PASSWORD" => Some(Keyword::Password),
            "NORECURSIVE" => Some(Keyword::Norecursive),
            "OF" => Some(Keyword::Of),
            "ALL" => Some(Keyword::All),
            "CAST" => Some(Keyword::Cast),
            "VECTOR" => Some(Keyword::Vector),
            "MASKED" => Some(Keyword::Masked),
            _ => None,
        }
    }
}

/// CQL lexer: converts input string into a sequence of tokens.
pub struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    /// Tokenize the entire input.
    pub fn tokenize(&mut self) -> Result<Vec<Token>, LexError> {
        let mut tokens = Vec::new();
        loop {
            let tok = self.next_token()?;
            let is_eof = tok.kind == TokenKind::Eof;
            tokens.push(tok);
            if is_eof {
                break;
            }
        }
        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<Token, LexError> {
        self.skip_whitespace_and_comments();

        if self.pos >= self.input.len() {
            return Ok(Token {
                kind: TokenKind::Eof,
                span: Span {
                    start: self.pos,
                    end: self.pos,
                },
            });
        }

        let start = self.pos;
        let ch = self.input[self.pos];

        // Single-char tokens.
        let kind = match ch {
            b'(' => {
                self.pos += 1;
                TokenKind::LParen
            }
            b')' => {
                self.pos += 1;
                TokenKind::RParen
            }
            b'{' => {
                self.pos += 1;
                TokenKind::LBrace
            }
            b'}' => {
                self.pos += 1;
                TokenKind::RBrace
            }
            b'[' => {
                self.pos += 1;
                TokenKind::LBracket
            }
            b']' => {
                self.pos += 1;
                TokenKind::RBracket
            }
            b',' => {
                self.pos += 1;
                TokenKind::Comma
            }
            b';' => {
                self.pos += 1;
                TokenKind::Semicolon
            }
            b'.' => {
                self.pos += 1;
                TokenKind::Dot
            }
            b'*' => {
                self.pos += 1;
                TokenKind::Star
            }
            b'+' => {
                self.pos += 1;
                TokenKind::Plus
            }
            b'/' => {
                self.pos += 1;
                TokenKind::Slash
            }
            b'%' => {
                self.pos += 1;
                TokenKind::Percent
            }
            b'?' => {
                self.pos += 1;
                TokenKind::QuestionMark
            }
            b'=' => {
                self.pos += 1;
                TokenKind::Eq
            }
            b'!' if self.peek(1) == Some(b'=') => {
                self.pos += 2;
                TokenKind::Neq
            }
            b'<' if self.peek(1) == Some(b'=') => {
                self.pos += 2;
                TokenKind::Lte
            }
            b'<' => {
                self.pos += 1;
                TokenKind::Lt
            }
            b'>' if self.peek(1) == Some(b'=') => {
                self.pos += 2;
                TokenKind::Gte
            }
            b'>' => {
                self.pos += 1;
                TokenKind::Gt
            }
            b':' if self
                .peek(1)
                .map_or(false, |b| b.is_ascii_alphanumeric() || b == b'_') =>
            {
                self.pos += 1;
                let name = self.read_ident_text();
                TokenKind::NamedBind(name)
            }
            b':' => {
                self.pos += 1;
                TokenKind::Colon
            }
            b'-' if self.peek(1).map_or(false, |b| b.is_ascii_digit()) => {
                return self.read_number(start);
            }
            b'-' => {
                self.pos += 1;
                TokenKind::Minus
            }
            b'\'' => return self.read_string_literal(start),
            b'"' => return self.read_quoted_ident(start),
            b'0' if self.peek(1) == Some(b'x') || self.peek(1) == Some(b'X') => {
                return self.read_blob_literal(start);
            }
            b'0'..=b'9' => return self.read_number(start),
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => return self.read_ident_or_keyword(start),
            _ => {
                return Err(LexError {
                    message: format!("unexpected character: '{}'", ch as char),
                    position: start,
                });
            }
        };

        Ok(Token {
            kind,
            span: Span {
                start,
                end: self.pos,
            },
        })
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace.
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
                self.pos += 1;
            }
            // Skip -- comments.
            if self.pos + 1 < self.input.len()
                && self.input[self.pos] == b'-'
                && self.input[self.pos + 1] == b'-'
            {
                while self.pos < self.input.len() && self.input[self.pos] != b'\n' {
                    self.pos += 1;
                }
                continue;
            }
            // Skip /* */ comments.
            if self.pos + 1 < self.input.len()
                && self.input[self.pos] == b'/'
                && self.input[self.pos + 1] == b'*'
            {
                self.pos += 2;
                while self.pos + 1 < self.input.len() {
                    if self.input[self.pos] == b'*' && self.input[self.pos + 1] == b'/' {
                        self.pos += 2;
                        break;
                    }
                    self.pos += 1;
                }
                continue;
            }
            break;
        }
    }

    fn peek(&self, offset: usize) -> Option<u8> {
        self.input.get(self.pos + offset).copied()
    }

    fn read_string_literal(&mut self, start: usize) -> Result<Token, LexError> {
        self.pos += 1; // skip opening quote
        let mut value = String::new();
        loop {
            if self.pos >= self.input.len() {
                return Err(LexError {
                    message: "unterminated string literal".to_string(),
                    position: start,
                });
            }
            if self.input[self.pos] == b'\'' {
                self.pos += 1;
                // Escaped quote: ''
                if self.pos < self.input.len() && self.input[self.pos] == b'\'' {
                    value.push('\'');
                    self.pos += 1;
                    continue;
                }
                break;
            }
            value.push(self.input[self.pos] as char);
            self.pos += 1;
        }
        Ok(Token {
            kind: TokenKind::StringLiteral(value),
            span: Span {
                start,
                end: self.pos,
            },
        })
    }

    fn read_quoted_ident(&mut self, start: usize) -> Result<Token, LexError> {
        self.pos += 1; // skip opening "
        let mut value = String::new();
        loop {
            if self.pos >= self.input.len() {
                return Err(LexError {
                    message: "unterminated quoted identifier".to_string(),
                    position: start,
                });
            }
            if self.input[self.pos] == b'"' {
                self.pos += 1;
                // Escaped: ""
                if self.pos < self.input.len() && self.input[self.pos] == b'"' {
                    value.push('"');
                    self.pos += 1;
                    continue;
                }
                break;
            }
            value.push(self.input[self.pos] as char);
            self.pos += 1;
        }
        Ok(Token {
            kind: TokenKind::QuotedIdent(value),
            span: Span {
                start,
                end: self.pos,
            },
        })
    }

    fn read_blob_literal(&mut self, start: usize) -> Result<Token, LexError> {
        self.pos += 2; // skip 0x
        let hex_start = self.pos;
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_hexdigit() {
            self.pos += 1;
        }
        let hex_str =
            std::str::from_utf8(&self.input[hex_start..self.pos]).map_err(|_| LexError {
                message: "invalid blob literal".to_string(),
                position: start,
            })?;
        let bytes = hex::decode(hex_str).map_err(|_| LexError {
            message: "invalid hex in blob literal".to_string(),
            position: start,
        })?;
        Ok(Token {
            kind: TokenKind::BlobLiteral(bytes),
            span: Span {
                start,
                end: self.pos,
            },
        })
    }

    fn read_number(&mut self, start: usize) -> Result<Token, LexError> {
        let neg = self.input[self.pos] == b'-';
        if neg {
            self.pos += 1;
        }
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        if self.pos < self.input.len() && self.input[self.pos] == b'.' {
            self.pos += 1;
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            let s = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
            let val = s.parse::<f64>().map_err(|_| LexError {
                message: format!("invalid float literal: {}", s),
                position: start,
            })?;
            return Ok(Token {
                kind: TokenKind::FloatLiteral(val),
                span: Span {
                    start,
                    end: self.pos,
                },
            });
        }
        // Check for UUID-like: if followed by '-' and hexdigit pattern
        let s = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
        let val = s.parse::<i64>().map_err(|_| LexError {
            message: format!("invalid integer literal: {}", s),
            position: start,
        })?;
        Ok(Token {
            kind: TokenKind::IntegerLiteral(val),
            span: Span {
                start,
                end: self.pos,
            },
        })
    }

    fn read_ident_text(&mut self) -> String {
        let start = self.pos;
        while self.pos < self.input.len()
            && (self.input[self.pos].is_ascii_alphanumeric() || self.input[self.pos] == b'_')
        {
            self.pos += 1;
        }
        String::from_utf8_lossy(&self.input[start..self.pos]).to_string()
    }

    fn read_ident_or_keyword(&mut self, start: usize) -> Result<Token, LexError> {
        // Read alphanumeric + underscore, then check for UUID pattern.
        let text = self.read_ident_text();

        // Check for UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        if text.len() == 8
            && text.chars().all(|c| c.is_ascii_hexdigit())
            && self.pos < self.input.len()
            && self.input[self.pos] == b'-'
        {
            // Try to read full UUID.
            let uuid_start = start;
            let remaining = &self.input[self.pos..];
            if remaining.len() >= 28 {
                // -xxxx-xxxx-xxxx-xxxxxxxxxxxx = 1+4+1+4+1+4+1+12 = 28
                let candidate = format!(
                    "{}{}",
                    text,
                    std::str::from_utf8(&remaining[..28]).unwrap_or("")
                );
                if is_uuid(&candidate) {
                    self.pos += 28;
                    return Ok(Token {
                        kind: TokenKind::UuidLiteral(candidate),
                        span: Span {
                            start: uuid_start,
                            end: self.pos,
                        },
                    });
                }
            }
        }

        // Check for boolean and null.
        match text.to_uppercase().as_str() {
            "TRUE" => {
                return Ok(Token {
                    kind: TokenKind::BooleanLiteral(true),
                    span: Span {
                        start,
                        end: self.pos,
                    },
                });
            }
            "FALSE" => {
                return Ok(Token {
                    kind: TokenKind::BooleanLiteral(false),
                    span: Span {
                        start,
                        end: self.pos,
                    },
                });
            }
            "NULL" => {
                return Ok(Token {
                    kind: TokenKind::NullLiteral,
                    span: Span {
                        start,
                        end: self.pos,
                    },
                });
            }
            _ => {}
        }

        // Check for keyword.
        if let Some(kw) = Keyword::from_str_ci(&text) {
            return Ok(Token {
                kind: TokenKind::Keyword(kw),
                span: Span {
                    start,
                    end: self.pos,
                },
            });
        }

        // Plain identifier (lowercase for case-insensitivity).
        Ok(Token {
            kind: TokenKind::Ident(text.to_lowercase()),
            span: Span {
                start,
                end: self.pos,
            },
        })
    }
}

fn is_uuid(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match i {
            8 | 13 | 18 | 23 => {
                if b != b'-' {
                    return false;
                }
            }
            _ => {
                if !b.is_ascii_hexdigit() {
                    return false;
                }
            }
        }
    }
    true
}

/// Lexer error with position information.
#[derive(Debug, Clone)]
pub struct LexError {
    pub message: String,
    pub position: usize,
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Lexer error at position {}: {}",
            self.position, self.message
        )
    }
}
impl std::error::Error for LexError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(input: &str) -> Vec<TokenKind> {
        let mut lexer = Lexer::new(input);
        lexer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|t| t.kind)
            .collect()
    }

    #[test]
    fn keywords() {
        let tokens = lex("SELECT FROM WHERE");
        assert_eq!(tokens[0], TokenKind::Keyword(Keyword::Select));
        assert_eq!(tokens[1], TokenKind::Keyword(Keyword::From));
        assert_eq!(tokens[2], TokenKind::Keyword(Keyword::Where));
    }

    #[test]
    fn case_insensitive_keywords() {
        let tokens = lex("select FROM Where");
        assert_eq!(tokens[0], TokenKind::Keyword(Keyword::Select));
        assert_eq!(tokens[1], TokenKind::Keyword(Keyword::From));
        assert_eq!(tokens[2], TokenKind::Keyword(Keyword::Where));
    }

    #[test]
    fn identifiers() {
        let tokens = lex("my_table MY_COL");
        assert_eq!(tokens[0], TokenKind::Ident("my_table".into()));
        assert_eq!(tokens[1], TokenKind::Ident("my_col".into()));
    }

    #[test]
    fn quoted_identifier() {
        let tokens = lex("\"MyTable\"");
        assert_eq!(tokens[0], TokenKind::QuotedIdent("MyTable".into()));
    }

    #[test]
    fn string_literal() {
        let tokens = lex("'hello world'");
        assert_eq!(tokens[0], TokenKind::StringLiteral("hello world".into()));
    }

    #[test]
    fn escaped_string() {
        let tokens = lex("'it''s'");
        assert_eq!(tokens[0], TokenKind::StringLiteral("it's".into()));
    }

    #[test]
    fn integer_literal() {
        let tokens = lex("42 -10");
        assert_eq!(tokens[0], TokenKind::IntegerLiteral(42));
        assert_eq!(tokens[1], TokenKind::IntegerLiteral(-10));
    }

    #[test]
    fn float_literal() {
        let tokens = lex("3.14");
        assert_eq!(tokens[0], TokenKind::FloatLiteral(3.14));
    }

    #[test]
    fn blob_literal() {
        let tokens = lex("0xDEADBEEF");
        assert_eq!(
            tokens[0],
            TokenKind::BlobLiteral(vec![0xDE, 0xAD, 0xBE, 0xEF])
        );
    }

    #[test]
    fn boolean_literals() {
        let tokens = lex("true false");
        assert_eq!(tokens[0], TokenKind::BooleanLiteral(true));
        assert_eq!(tokens[1], TokenKind::BooleanLiteral(false));
    }

    #[test]
    fn null_literal() {
        let tokens = lex("null");
        assert_eq!(tokens[0], TokenKind::NullLiteral);
    }

    #[test]
    fn bind_markers() {
        let tokens = lex("? :name");
        assert_eq!(tokens[0], TokenKind::QuestionMark);
        assert_eq!(tokens[1], TokenKind::NamedBind("name".into()));
    }

    #[test]
    fn punctuation() {
        let tokens = lex("( ) , ; . * = < > <=");
        assert_eq!(tokens[0], TokenKind::LParen);
        assert_eq!(tokens[1], TokenKind::RParen);
        assert_eq!(tokens[2], TokenKind::Comma);
        assert_eq!(tokens[3], TokenKind::Semicolon);
        assert_eq!(tokens[4], TokenKind::Dot);
        assert_eq!(tokens[5], TokenKind::Star);
        assert_eq!(tokens[6], TokenKind::Eq);
        assert_eq!(tokens[7], TokenKind::Lt);
        assert_eq!(tokens[8], TokenKind::Gt);
        assert_eq!(tokens[9], TokenKind::Lte);
    }

    #[test]
    fn full_select() {
        let tokens = lex("SELECT id, name FROM users WHERE id = ?");
        assert!(tokens.len() > 5);
        assert_eq!(tokens[0], TokenKind::Keyword(Keyword::Select));
        assert_eq!(tokens[1], TokenKind::Ident("id".into()));
    }

    #[test]
    fn comments() {
        let tokens = lex("SELECT -- comment\n* FROM t");
        assert_eq!(tokens[0], TokenKind::Keyword(Keyword::Select));
        assert_eq!(tokens[1], TokenKind::Star);
    }

    #[test]
    fn block_comments() {
        let tokens = lex("SELECT /* block */ * FROM t");
        assert_eq!(tokens[0], TokenKind::Keyword(Keyword::Select));
        assert_eq!(tokens[1], TokenKind::Star);
    }

    #[test]
    fn unterminated_string_error() {
        let mut lexer = Lexer::new("'unterminated");
        assert!(lexer.tokenize().is_err());
    }

    #[test]
    fn create_keyspace() {
        let tokens = lex(
            "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        );
        assert_eq!(tokens[0], TokenKind::Keyword(Keyword::Create));
        assert_eq!(tokens[1], TokenKind::Keyword(Keyword::Keyspace));
        assert_eq!(tokens[2], TokenKind::Ident("test".into()));
    }
}
