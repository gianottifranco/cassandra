// Licensed under Apache License, Version 2.0.

//! CQL recursive-descent parser.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.CqlParser` (ANTLR-generated)
//!
//! ## Architecture
//! Hand-rolled recursive descent. Each parse_* method corresponds
//! to a grammar production. Errors include position information.

use std::collections::HashMap;
use crate::ast::*;
use crate::lexer::{Keyword, Lexer, Token, TokenKind};

/// Parse a CQL statement string into an AST.
pub fn parse(input: &str) -> Result<Statement, ParseError> {
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().map_err(|e| ParseError {
        message: e.message,
        position: e.position,
    })?;
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement()?;
    // Allow trailing semicolons.
    parser.eat_if(TokenKind::Semicolon);
    parser.expect_eof()?;
    Ok(stmt)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn current(&self) -> &Token {
        &self.tokens[self.pos.min(self.tokens.len() - 1)]
    }

    fn peek_kind(&self) -> &TokenKind {
        &self.current().kind
    }

    fn advance(&mut self) -> &Token {
        let tok = &self.tokens[self.pos.min(self.tokens.len() - 1)];
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn eat_if(&mut self, kind: TokenKind) -> bool {
        if *self.peek_kind() == kind {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, kind: TokenKind) -> Result<&Token, ParseError> {
        if *self.peek_kind() == kind {
            Ok(self.advance())
        } else {
            Err(self.error(format!("expected {}, got {}", kind, self.peek_kind())))
        }
    }

    fn expect_keyword(&mut self, kw: Keyword) -> Result<(), ParseError> {
        if *self.peek_kind() == TokenKind::Keyword(kw) {
            self.advance();
            Ok(())
        } else {
            Err(self.error(format!("expected {:?}, got {}", kw, self.peek_kind())))
        }
    }

    fn eat_keyword(&mut self, kw: Keyword) -> bool {
        if *self.peek_kind() == TokenKind::Keyword(kw) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::Ident(s) => {
                let s = s.clone();
                self.advance();
                Ok(s)
            }
            TokenKind::QuotedIdent(s) => {
                let s = s.clone();
                self.advance();
                Ok(s)
            }
            // Allow some keywords as identifiers in non-reserved positions.
            TokenKind::Keyword(kw) => {
                if is_unreserved_keyword(kw) {
                    let name = format!("{:?}", kw).to_lowercase();
                    self.advance();
                    Ok(name)
                } else {
                    Err(self.error(format!("expected identifier, got keyword {:?}", kw)))
                }
            }
            _ => Err(self.error(format!("expected identifier, got {}", self.peek_kind()))),
        }
    }

    fn expect_eof(&self) -> Result<(), ParseError> {
        if *self.peek_kind() == TokenKind::Eof {
            Ok(())
        } else {
            Err(self.error(format!("unexpected token: {}", self.peek_kind())))
        }
    }

    fn error(&self, message: String) -> ParseError {
        ParseError {
            message,
            position: self.current().span.start,
        }
    }

    // ─── Top-level dispatch ─────────────────────────────────────────────

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::Keyword(Keyword::Select) => self.parse_select(),
            TokenKind::Keyword(Keyword::Insert) => self.parse_insert(),
            TokenKind::Keyword(Keyword::Update) => self.parse_update(),
            TokenKind::Keyword(Keyword::Delete) => self.parse_delete(),
            TokenKind::Keyword(Keyword::Create) => self.parse_create(),
            TokenKind::Keyword(Keyword::Alter) => self.parse_alter(),
            TokenKind::Keyword(Keyword::Drop) => self.parse_drop(),
            TokenKind::Keyword(Keyword::Use) => self.parse_use(),
            TokenKind::Keyword(Keyword::Truncate) => self.parse_truncate(),
            TokenKind::Keyword(Keyword::Begin) => self.parse_batch(),
            _ => Err(self.error(format!("unexpected token: {}", self.peek_kind()))),
        }
    }

    // ─── USE ────────────────────────────────────────────────────────────

    fn parse_use(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Use)?;
        let ks = self.expect_ident()?;
        Ok(Statement::Use(UseStatement { keyspace: ks }))
    }

    // ─── TRUNCATE ───────────────────────────────────────────────────────

    fn parse_truncate(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Truncate)?;
        self.eat_keyword(Keyword::Table); // optional TABLE keyword
        let (ks, name) = self.parse_table_name()?;
        Ok(Statement::Truncate(TruncateStatement {
            keyspace: ks,
            table: name,
        }))
    }

    // ─── CREATE ─────────────────────────────────────────────────────────

    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Create)?;
        match self.peek_kind().clone() {
            TokenKind::Keyword(Keyword::Keyspace) => self.parse_create_keyspace(),
            TokenKind::Keyword(Keyword::Table) => self.parse_create_table(),
            _ => Err(self.error(format!("expected KEYSPACE or TABLE after CREATE, got {}", self.peek_kind()))),
        }
    }

    fn parse_create_keyspace(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Keyspace)?;
        let if_not_exists = self.parse_if_not_exists();
        let name = self.expect_ident()?;
        self.expect_keyword(Keyword::With)?;

        let mut replication = HashMap::new();
        let mut durable_writes = None;

        // Parse WITH options.
        loop {
            if self.eat_keyword(Keyword::Replication) {
                self.expect(TokenKind::Eq)?;
                replication = self.parse_map_literal_strings()?;
            } else if self.eat_keyword(Keyword::DurableWrites) {
                self.expect(TokenKind::Eq)?;
                match self.peek_kind() {
                    TokenKind::BooleanLiteral(b) => {
                        durable_writes = Some(*b);
                        self.advance();
                    }
                    _ => return Err(self.error("expected boolean for durable_writes".into())),
                }
            } else {
                break;
            }
            if !self.eat_keyword(Keyword::And) {
                break;
            }
        }

        Ok(Statement::CreateKeyspace(CreateKeyspace {
            name,
            if_not_exists,
            replication,
            durable_writes,
        }))
    }

    fn parse_create_table(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Table)?;
        let if_not_exists = self.parse_if_not_exists();
        let (ks, name) = self.parse_table_name()?;

        self.expect(TokenKind::LParen)?;

        let mut columns = Vec::new();
        let mut inline_pk: Option<String> = None;
        let mut partition_key = Vec::new();
        let mut clustering_key = Vec::new();

        loop {
            if *self.peek_kind() == TokenKind::Keyword(Keyword::Primary) {
                // PRIMARY KEY definition
                self.expect_keyword(Keyword::Primary)?;
                self.expect_keyword(Keyword::Key)?;
                self.expect(TokenKind::LParen)?;

                // Partition key: either (a) or ((a, b))
                if self.eat_if(TokenKind::LParen) {
                    // Composite partition key
                    partition_key = self.parse_ident_list()?;
                    self.expect(TokenKind::RParen)?;
                } else {
                    partition_key.push(self.expect_ident()?);
                }

                // Clustering columns
                while self.eat_if(TokenKind::Comma) {
                    clustering_key.push(self.expect_ident()?);
                }

                self.expect(TokenKind::RParen)?;
            } else {
                let col_name = self.expect_ident()?;
                let cql_type = self.parse_cql_type()?;
                let is_static = self.eat_keyword(Keyword::Static);
                let is_pk = self.eat_keyword(Keyword::Primary) && {
                    self.expect_keyword(Keyword::Key)?;
                    true
                };
                if is_pk {
                    inline_pk = Some(col_name.clone());
                }
                columns.push(ColumnDef {
                    name: col_name,
                    cql_type,
                    is_static,
                });
            }

            if !self.eat_if(TokenKind::Comma) {
                break;
            }
        }

        self.expect(TokenKind::RParen)?;

        // If inline PK used, set partition key.
        if let Some(pk) = inline_pk {
            if partition_key.is_empty() {
                partition_key.push(pk);
            }
        }

        let mut clustering_order = Vec::new();
        let mut options = HashMap::new();
        let mut compact_storage = false;

        if self.eat_keyword(Keyword::With) {
            loop {
                if self.eat_keyword(Keyword::Clustering) {
                    self.expect_keyword(Keyword::Order)?;
                    self.expect_keyword(Keyword::By)?;
                    self.expect(TokenKind::LParen)?;
                    loop {
                        let col = self.expect_ident()?;
                        let order = if self.eat_keyword(Keyword::Desc) {
                            ClusteringOrder::Desc
                        } else {
                            self.eat_keyword(Keyword::Asc);
                            ClusteringOrder::Asc
                        };
                        clustering_order.push((col, order));
                        if !self.eat_if(TokenKind::Comma) {
                            break;
                        }
                    }
                    self.expect(TokenKind::RParen)?;
                } else if self.eat_keyword(Keyword::Compact) {
                    self.expect_keyword(Keyword::Storage)?;
                    compact_storage = true;
                } else {
                    let key = self.expect_ident()?;
                    self.expect(TokenKind::Eq)?;
                    let val = self.parse_option_value()?;
                    options.insert(key, val);
                }
                if !self.eat_keyword(Keyword::And) {
                    break;
                }
            }
        }

        Ok(Statement::CreateTable(CreateTable {
            keyspace: ks,
            name,
            if_not_exists,
            columns,
            partition_key,
            clustering_key,
            clustering_order,
            options,
            compact_storage,
        }))
    }

    // ─── ALTER ───────────────────────────────────────────────────────────

    fn parse_alter(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Alter)?;
        match self.peek_kind().clone() {
            TokenKind::Keyword(Keyword::Keyspace) => {
                self.expect_keyword(Keyword::Keyspace)?;
                let name = self.expect_ident()?;
                self.expect_keyword(Keyword::With)?;
                let mut replication = None;
                let mut durable_writes = None;
                loop {
                    if self.eat_keyword(Keyword::Replication) {
                        self.expect(TokenKind::Eq)?;
                        replication = Some(self.parse_map_literal_strings()?);
                    } else if self.eat_keyword(Keyword::DurableWrites) {
                        self.expect(TokenKind::Eq)?;
                        match self.peek_kind() {
                            TokenKind::BooleanLiteral(b) => {
                                durable_writes = Some(*b);
                                self.advance();
                            }
                            _ => return Err(self.error("expected boolean".into())),
                        }
                    } else {
                        break;
                    }
                    if !self.eat_keyword(Keyword::And) { break; }
                }
                Ok(Statement::AlterKeyspace(AlterKeyspace { name, replication, durable_writes }))
            }
            TokenKind::Keyword(Keyword::Table) => {
                self.expect_keyword(Keyword::Table)?;
                let (ks, name) = self.parse_table_name()?;
                let operation = if self.eat_keyword(Keyword::Add) {
                    let col_name = self.expect_ident()?;
                    let cql_type = self.parse_cql_type()?;
                    let is_static = self.eat_keyword(Keyword::Static);
                    AlterTableOp::AddColumn(ColumnDef { name: col_name, cql_type, is_static })
                } else if self.eat_keyword(Keyword::Drop) {
                    let col_name = self.expect_ident()?;
                    AlterTableOp::DropColumn(col_name)
                } else if self.eat_keyword(Keyword::With) {
                    let mut opts = HashMap::new();
                    loop {
                        let key = self.expect_ident()?;
                        self.expect(TokenKind::Eq)?;
                        let val = self.parse_option_value()?;
                        opts.insert(key, val);
                        if !self.eat_keyword(Keyword::And) { break; }
                    }
                    AlterTableOp::WithOptions(opts)
                } else {
                    return Err(self.error("expected ADD, DROP, or WITH after ALTER TABLE".into()));
                };
                Ok(Statement::AlterTable(AlterTable { keyspace: ks, name, operation }))
            }
            _ => Err(self.error("expected KEYSPACE or TABLE after ALTER".into())),
        }
    }

    // ─── DROP ───────────────────────────────────────────────────────────

    fn parse_drop(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Drop)?;
        match self.peek_kind().clone() {
            TokenKind::Keyword(Keyword::Keyspace) => {
                self.expect_keyword(Keyword::Keyspace)?;
                let if_exists = self.parse_if_exists();
                let name = self.expect_ident()?;
                Ok(Statement::DropKeyspace(DropKeyspace { name, if_exists }))
            }
            TokenKind::Keyword(Keyword::Table) => {
                self.expect_keyword(Keyword::Table)?;
                let if_exists = self.parse_if_exists();
                let (ks, name) = self.parse_table_name()?;
                Ok(Statement::DropTable(DropTable { keyspace: ks, name, if_exists }))
            }
            _ => Err(self.error("expected KEYSPACE or TABLE after DROP".into())),
        }
    }

    // ─── SELECT ─────────────────────────────────────────────────────────

    fn parse_select(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Select)?;
        let json = self.eat_keyword(Keyword::Json);
        let distinct = self.eat_keyword(Keyword::Distinct);

        let columns = if self.eat_if(TokenKind::Star) {
            SelectColumns::All
        } else {
            let mut selectors = Vec::new();
            loop {
                selectors.push(self.parse_selector()?);
                if !self.eat_if(TokenKind::Comma) {
                    break;
                }
            }
            SelectColumns::Named(selectors)
        };

        self.expect_keyword(Keyword::From)?;
        let (ks, table) = self.parse_table_name()?;

        let where_clause = if self.eat_keyword(Keyword::Where) {
            self.parse_where_clause()?
        } else {
            Vec::new()
        };

        let mut order_by = Vec::new();
        if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            loop {
                let col = self.expect_ident()?;
                let order = if self.eat_keyword(Keyword::Desc) {
                    ClusteringOrder::Desc
                } else {
                    self.eat_keyword(Keyword::Asc);
                    ClusteringOrder::Asc
                };
                order_by.push((col, order));
                if !self.eat_if(TokenKind::Comma) {
                    break;
                }
            }
        }

        let mut limit = None;
        let mut per_partition_limit = None;

        if self.eat_keyword(Keyword::Per) {
            self.expect_keyword(Keyword::Partition)?;
            self.expect_keyword(Keyword::Limit)?;
            per_partition_limit = Some(self.parse_term()?);
        }

        if self.eat_keyword(Keyword::Limit) {
            limit = Some(self.parse_term()?);
        }

        let allow_filtering = self.eat_keyword(Keyword::Allow) && {
            self.expect_keyword(Keyword::Filtering)?;
            true
        };

        Ok(Statement::Select(Select {
            distinct,
            json,
            columns,
            keyspace: ks,
            table,
            where_clause,
            order_by,
            limit,
            per_partition_limit,
            allow_filtering,
        }))
    }

    fn parse_selector(&mut self) -> Result<Selector, ParseError> {
        let name = self.expect_ident()?;
        // Check for function call.
        if self.eat_if(TokenKind::LParen) {
            if *self.peek_kind() == TokenKind::Star {
                self.advance();
                self.expect(TokenKind::RParen)?;
                return Ok(Selector::Count);
            }
            let mut args = Vec::new();
            if *self.peek_kind() != TokenKind::RParen {
                loop {
                    args.push(self.parse_selector()?);
                    if !self.eat_if(TokenKind::Comma) {
                        break;
                    }
                }
            }
            self.expect(TokenKind::RParen)?;
            let sel = Selector::Function(name, args);
            if self.eat_keyword(Keyword::As) {
                let alias = self.expect_ident()?;
                return Ok(Selector::Alias {
                    selector: Box::new(sel),
                    alias,
                });
            }
            return Ok(sel);
        }
        if self.eat_keyword(Keyword::As) {
            let alias = self.expect_ident()?;
            return Ok(Selector::Alias {
                selector: Box::new(Selector::Column(name)),
                alias,
            });
        }
        Ok(Selector::Column(name))
    }

    // ─── INSERT ─────────────────────────────────────────────────────────

    fn parse_insert(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;
        let (ks, table) = self.parse_table_name()?;

        let mut if_not_exists = false;

        if self.eat_keyword(Keyword::Json) {
            let json_val = self.parse_term()?;
            if self.eat_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Not)?;
                self.expect_keyword(Keyword::Exists)?;
                if_not_exists = true;
            }
            let using = self.parse_using()?;
            return Ok(Statement::Insert(Insert {
                keyspace: ks,
                table,
                if_not_exists,
                columns: Vec::new(),
                values: Vec::new(),
                json: Some(json_val),
                using,
            }));
        }

        self.expect(TokenKind::LParen)?;
        let columns = self.parse_ident_list()?;
        self.expect(TokenKind::RParen)?;

        self.expect_keyword(Keyword::Values)?;
        self.expect(TokenKind::LParen)?;
        let mut values = Vec::new();
        loop {
            values.push(self.parse_term()?);
            if !self.eat_if(TokenKind::Comma) {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;

        if self.eat_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            if_not_exists = true;
        }

        let using = self.parse_using()?;

        Ok(Statement::Insert(Insert {
            keyspace: ks,
            table,
            if_not_exists,
            columns,
            values,
            json: None,
            using,
        }))
    }

    // ─── UPDATE ─────────────────────────────────────────────────────────

    fn parse_update(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Update)?;
        let (ks, table) = self.parse_table_name()?;
        let using = self.parse_using()?;

        self.expect_keyword(Keyword::Set)?;
        let mut assignments = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.expect(TokenKind::Eq)?;
            let val = self.parse_term()?;
            assignments.push(Assignment { column: col, value: val });
            if !self.eat_if(TokenKind::Comma) {
                break;
            }
        }

        self.expect_keyword(Keyword::Where)?;
        let where_clause = self.parse_where_clause()?;

        let mut if_exists = false;
        let mut if_conditions = Vec::new();
        if self.eat_keyword(Keyword::If) {
            if self.eat_keyword(Keyword::Exists) {
                if_exists = true;
            } else {
                if_conditions = self.parse_where_clause()?;
            }
        }

        Ok(Statement::Update(Update {
            keyspace: ks,
            table,
            using,
            assignments,
            where_clause,
            if_exists,
            if_conditions,
        }))
    }

    // ─── DELETE ─────────────────────────────────────────────────────────

    fn parse_delete(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Delete)?;

        let mut columns = Vec::new();
        // Column list before FROM (optional).
        if *self.peek_kind() != TokenKind::Keyword(Keyword::From) {
            loop {
                columns.push(self.expect_ident()?);
                if !self.eat_if(TokenKind::Comma) {
                    break;
                }
            }
        }

        self.expect_keyword(Keyword::From)?;
        let (ks, table) = self.parse_table_name()?;
        let using = self.parse_using()?;

        self.expect_keyword(Keyword::Where)?;
        let where_clause = self.parse_where_clause()?;

        let mut if_exists = false;
        let mut if_conditions = Vec::new();
        if self.eat_keyword(Keyword::If) {
            if self.eat_keyword(Keyword::Exists) {
                if_exists = true;
            } else {
                if_conditions = self.parse_where_clause()?;
            }
        }

        Ok(Statement::Delete(Delete {
            columns,
            keyspace: ks,
            table,
            using,
            where_clause,
            if_exists,
            if_conditions,
        }))
    }

    // ─── BATCH ──────────────────────────────────────────────────────────

    fn parse_batch(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Begin)?;

        let batch_type = if self.eat_keyword(Keyword::Unlogged) {
            BatchType::Unlogged
        } else if self.eat_keyword(Keyword::Counter) {
            BatchType::Counter
        } else {
            BatchType::Logged
        };

        self.expect_keyword(Keyword::Batch)?;
        let using = self.parse_using()?;

        let mut statements = Vec::new();
        loop {
            if *self.peek_kind() == TokenKind::Keyword(Keyword::Apply) {
                break;
            }
            let stmt = self.parse_statement()?;
            self.eat_if(TokenKind::Semicolon);
            statements.push(stmt);
        }

        self.expect_keyword(Keyword::Apply)?;
        self.expect_keyword(Keyword::Batch)?;

        Ok(Statement::Batch(BatchStatement {
            batch_type,
            statements,
            using,
        }))
    }

    // ─── Helpers ────────────────────────────────────────────────────────

    fn parse_table_name(&mut self) -> Result<(Option<String>, String), ParseError> {
        let first = self.expect_ident()?;
        if self.eat_if(TokenKind::Dot) {
            let second = self.expect_ident()?;
            Ok((Some(first), second))
        } else {
            Ok((None, first))
        }
    }

    fn parse_if_not_exists(&mut self) -> bool {
        if self.eat_keyword(Keyword::If) {
            if self.eat_keyword(Keyword::Not) {
                let _ = self.expect_keyword(Keyword::Exists);
                return true;
            }
        }
        false
    }

    fn parse_if_exists(&mut self) -> bool {
        if self.eat_keyword(Keyword::If) {
            if self.eat_keyword(Keyword::Exists) {
                return true;
            }
        }
        false
    }

    fn parse_ident_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut list = Vec::new();
        loop {
            list.push(self.expect_ident()?);
            if !self.eat_if(TokenKind::Comma) {
                break;
            }
        }
        Ok(list)
    }

    fn parse_where_clause(&mut self) -> Result<Vec<Relation>, ParseError> {
        let mut relations = Vec::new();
        loop {
            let col = self.expect_ident()?;
            let op = self.parse_relation_op()?;
            let value = self.parse_term()?;
            relations.push(Relation { column: col, op, value });
            if !self.eat_keyword(Keyword::And) {
                break;
            }
        }
        Ok(relations)
    }

    fn parse_relation_op(&mut self) -> Result<RelationOp, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::Eq => { self.advance(); Ok(RelationOp::Eq) }
            TokenKind::Neq => { self.advance(); Ok(RelationOp::Neq) }
            TokenKind::Lt => { self.advance(); Ok(RelationOp::Lt) }
            TokenKind::Gt => { self.advance(); Ok(RelationOp::Gt) }
            TokenKind::Lte => { self.advance(); Ok(RelationOp::Lte) }
            TokenKind::Gte => { self.advance(); Ok(RelationOp::Gte) }
            TokenKind::Keyword(Keyword::In) => { self.advance(); Ok(RelationOp::In) }
            TokenKind::Keyword(Keyword::Contains) => {
                self.advance();
                if self.eat_keyword(Keyword::Key) {
                    Ok(RelationOp::ContainsKey)
                } else {
                    Ok(RelationOp::Contains)
                }
            }
            _ => Err(self.error(format!("expected comparison operator, got {}", self.peek_kind()))),
        }
    }

    fn parse_term(&mut self) -> Result<Term, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::StringLiteral(s) => {
                let s = s.clone();
                self.advance();
                Ok(Term::Literal(Literal::String(s)))
            }
            TokenKind::IntegerLiteral(n) => {
                self.advance();
                Ok(Term::Literal(Literal::Integer(n)))
            }
            TokenKind::FloatLiteral(f) => {
                self.advance();
                Ok(Term::Literal(Literal::Float(f)))
            }
            TokenKind::BlobLiteral(b) => {
                let b = b.clone();
                self.advance();
                Ok(Term::Literal(Literal::Blob(b)))
            }
            TokenKind::UuidLiteral(s) => {
                let s = s.clone();
                self.advance();
                Ok(Term::Literal(Literal::Uuid(s)))
            }
            TokenKind::BooleanLiteral(b) => {
                self.advance();
                Ok(Term::Literal(Literal::Boolean(b)))
            }
            TokenKind::NullLiteral => {
                self.advance();
                Ok(Term::Literal(Literal::Null))
            }
            TokenKind::QuestionMark => {
                self.advance();
                Ok(Term::BindMarker(BindMarker::Anonymous))
            }
            TokenKind::NamedBind(name) => {
                let name = name.clone();
                self.advance();
                Ok(Term::BindMarker(BindMarker::Named(name)))
            }
            TokenKind::LParen => {
                self.advance();
                let mut terms = Vec::new();
                if *self.peek_kind() != TokenKind::RParen {
                    loop {
                        terms.push(self.parse_term()?);
                        if !self.eat_if(TokenKind::Comma) {
                            break;
                        }
                    }
                }
                self.expect(TokenKind::RParen)?;
                Ok(Term::TupleLiteral(terms))
            }
            TokenKind::LBracket => {
                self.advance();
                let mut items = Vec::new();
                if *self.peek_kind() != TokenKind::RBracket {
                    loop {
                        items.push(self.parse_term()?);
                        if !self.eat_if(TokenKind::Comma) {
                            break;
                        }
                    }
                }
                self.expect(TokenKind::RBracket)?;
                Ok(Term::CollectionLiteral(items))
            }
            TokenKind::LBrace => {
                self.advance();
                let mut entries = Vec::new();
                if *self.peek_kind() != TokenKind::RBrace {
                    loop {
                        let key = self.parse_term()?;
                        self.expect(TokenKind::Colon)?;
                        let val = self.parse_term()?;
                        entries.push((key, val));
                        if !self.eat_if(TokenKind::Comma) {
                            break;
                        }
                    }
                }
                self.expect(TokenKind::RBrace)?;
                Ok(Term::MapLiteral(entries))
            }
            TokenKind::Ident(name) => {
                let name = name.clone();
                self.advance();
                if self.eat_if(TokenKind::LParen) {
                    let mut args = Vec::new();
                    if *self.peek_kind() != TokenKind::RParen {
                        loop {
                            args.push(self.parse_term()?);
                            if !self.eat_if(TokenKind::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(TokenKind::RParen)?;
                    Ok(Term::FunctionCall(name, args))
                } else {
                    // Treat unknown identifier as a string-like token
                    // (for unquoted function args, column refs in conditions, etc.)
                    Ok(Term::Literal(Literal::String(name)))
                }
            }
            TokenKind::Minus => {
                self.advance();
                if let TokenKind::IntegerLiteral(n) = self.peek_kind() {
                    let n = *n;
                    self.advance();
                    Ok(Term::Literal(Literal::Integer(-n)))
                } else {
                    Err(self.error("expected number after minus".into()))
                }
            }
            _ => Err(self.error(format!("expected value or bind marker, got {}", self.peek_kind()))),
        }
    }

    fn parse_cql_type(&mut self) -> Result<CqlTypeName, ParseError> {
        if self.eat_keyword(Keyword::Frozen) {
            self.expect(TokenKind::Lt)?;
            let inner = self.parse_cql_type()?;
            self.expect(TokenKind::Gt)?;
            return Ok(CqlTypeName::Frozen(Box::new(inner)));
        }

        if self.eat_keyword(Keyword::List) {
            self.expect(TokenKind::Lt)?;
            let inner = self.parse_cql_type()?;
            self.expect(TokenKind::Gt)?;
            return Ok(CqlTypeName::List(Box::new(inner)));
        }

        if self.eat_keyword(Keyword::Set) {
            self.expect(TokenKind::Lt)?;
            let inner = self.parse_cql_type()?;
            self.expect(TokenKind::Gt)?;
            return Ok(CqlTypeName::Set(Box::new(inner)));
        }

        if self.eat_keyword(Keyword::Map) {
            self.expect(TokenKind::Lt)?;
            let key_type = self.parse_cql_type()?;
            self.expect(TokenKind::Comma)?;
            let val_type = self.parse_cql_type()?;
            self.expect(TokenKind::Gt)?;
            return Ok(CqlTypeName::Map(Box::new(key_type), Box::new(val_type)));
        }

        // TODO: tuple<...>

        let name = self.expect_ident()?;
        Ok(CqlTypeName::Simple(name))
    }

    fn parse_map_literal_strings(&mut self) -> Result<HashMap<String, String>, ParseError> {
        self.expect(TokenKind::LBrace)?;
        let mut map = HashMap::new();
        if *self.peek_kind() != TokenKind::RBrace {
            loop {
                let key = match self.peek_kind().clone() {
                    TokenKind::StringLiteral(s) => { self.advance(); s }
                    _ => return Err(self.error("expected string key in map".into())),
                };
                self.expect(TokenKind::Colon)?;
                let value = match self.peek_kind().clone() {
                    TokenKind::StringLiteral(s) => { self.advance(); s }
                    TokenKind::IntegerLiteral(n) => { self.advance(); n.to_string() }
                    _ => return Err(self.error("expected string or integer value in map".into())),
                };
                map.insert(key, value);
                if !self.eat_if(TokenKind::Comma) {
                    break;
                }
            }
        }
        self.expect(TokenKind::RBrace)?;
        Ok(map)
    }

    fn parse_option_value(&mut self) -> Result<String, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::StringLiteral(s) => { self.advance(); Ok(s) }
            TokenKind::IntegerLiteral(n) => { self.advance(); Ok(n.to_string()) }
            TokenKind::FloatLiteral(f) => { self.advance(); Ok(f.to_string()) }
            TokenKind::BooleanLiteral(b) => { self.advance(); Ok(b.to_string()) }
            TokenKind::LBrace => {
                let map = self.parse_map_literal_strings()?;
                Ok(format!("{:?}", map))
            }
            _ => Err(self.error(format!("expected option value, got {}", self.peek_kind()))),
        }
    }

    fn parse_using(&mut self) -> Result<Vec<UsingClause>, ParseError> {
        let mut clauses = Vec::new();
        if self.eat_keyword(Keyword::Using) {
            loop {
                if self.eat_keyword(Keyword::Timestamp) {
                    clauses.push(UsingClause::Timestamp(self.parse_term()?));
                } else if self.eat_keyword(Keyword::Ttl) {
                    clauses.push(UsingClause::Ttl(self.parse_term()?));
                } else {
                    break;
                }
                if !self.eat_keyword(Keyword::And) {
                    break;
                }
            }
        }
        Ok(clauses)
    }
}

fn is_unreserved_keyword(kw: Keyword) -> bool {
    matches!(
        kw,
        Keyword::Json
            | Keyword::Clustering
            | Keyword::Compact
            | Keyword::Storage
            | Keyword::Frozen
            | Keyword::List
            | Keyword::Map
            | Keyword::Set
            | Keyword::Tuple
            | Keyword::Type
            | Keyword::Key
            | Keyword::Timestamp
            | Keyword::Ttl
            | Keyword::Token
            | Keyword::Filtering
            | Keyword::Contains
            | Keyword::Static
            | Keyword::Index
            | Keyword::Add
            | Keyword::Rename
            | Keyword::Column
            | Keyword::View
            | Keyword::As
            | Keyword::Per
            | Keyword::Partition
            | Keyword::Group
            | Keyword::Replication
            | Keyword::Counter
            | Keyword::DurableWrites
    )
}

/// Parser error.
#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Parse error at position {}: {}", self.position, self.message)
    }
}
impl std::error::Error for ParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_use() {
        let stmt = parse("USE my_keyspace").unwrap();
        match stmt {
            Statement::Use(u) => assert_eq!(u.keyspace, "my_keyspace"),
            _ => panic!("expected Use"),
        }
    }

    #[test]
    fn parse_create_keyspace() {
        let stmt = parse(
            "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"
        ).unwrap();
        match stmt {
            Statement::CreateKeyspace(ck) => {
                assert_eq!(ck.name, "test");
                assert!(!ck.if_not_exists);
                assert_eq!(ck.replication.get("class").unwrap(), "SimpleStrategy");
            }
            _ => panic!("expected CreateKeyspace"),
        }
    }

    #[test]
    fn parse_create_keyspace_if_not_exists() {
        let stmt = parse(
            "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"
        ).unwrap();
        match stmt {
            Statement::CreateKeyspace(ck) => assert!(ck.if_not_exists),
            _ => panic!("expected CreateKeyspace"),
        }
    }

    #[test]
    fn parse_create_table() {
        let stmt = parse(
            "CREATE TABLE ks.users (id uuid, name text, age int, PRIMARY KEY (id))"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.keyspace, Some("ks".into()));
                assert_eq!(ct.name, "users");
                assert_eq!(ct.columns.len(), 3);
                assert_eq!(ct.partition_key, vec!["id"]);
                assert!(ct.clustering_key.is_empty());
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_create_table_composite_pk() {
        let stmt = parse(
            "CREATE TABLE t (a int, b int, c int, d int, PRIMARY KEY ((a, b), c))"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.partition_key, vec!["a", "b"]);
                assert_eq!(ct.clustering_key, vec!["c"]);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_create_table_inline_pk() {
        let stmt = parse("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.partition_key, vec!["id"]);
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_select_all() {
        let stmt = parse("SELECT * FROM users").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.table, "users");
                assert!(matches!(s.columns, SelectColumns::All));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_with_where() {
        let stmt = parse("SELECT id, name FROM users WHERE id = ?").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.where_clause.len(), 1);
                assert_eq!(s.where_clause[0].column, "id");
                assert_eq!(s.where_clause[0].op, RelationOp::Eq);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_with_limit() {
        let stmt = parse("SELECT * FROM t LIMIT 10").unwrap();
        match stmt {
            Statement::Select(s) => assert!(s.limit.is_some()),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_insert() {
        let stmt = parse("INSERT INTO t (id, name) VALUES (?, 'Alice')").unwrap();
        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.columns, vec!["id", "name"]);
                assert_eq!(i.values.len(), 2);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_update() {
        let stmt = parse("UPDATE t SET name = 'Bob' WHERE id = ?").unwrap();
        match stmt {
            Statement::Update(u) => {
                assert_eq!(u.assignments.len(), 1);
                assert_eq!(u.assignments[0].column, "name");
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn parse_delete() {
        let stmt = parse("DELETE FROM t WHERE id = ?").unwrap();
        match stmt {
            Statement::Delete(d) => {
                assert!(d.columns.is_empty());
                assert_eq!(d.where_clause.len(), 1);
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn parse_drop_keyspace() {
        let stmt = parse("DROP KEYSPACE IF EXISTS test").unwrap();
        match stmt {
            Statement::DropKeyspace(dk) => {
                assert_eq!(dk.name, "test");
                assert!(dk.if_exists);
            }
            _ => panic!("expected DropKeyspace"),
        }
    }

    #[test]
    fn parse_drop_table() {
        let stmt = parse("DROP TABLE ks.t").unwrap();
        match stmt {
            Statement::DropTable(dt) => {
                assert_eq!(dt.keyspace, Some("ks".into()));
                assert_eq!(dt.name, "t");
            }
            _ => panic!("expected DropTable"),
        }
    }

    #[test]
    fn parse_truncate() {
        let stmt = parse("TRUNCATE TABLE ks.t").unwrap();
        match stmt {
            Statement::Truncate(t) => {
                assert_eq!(t.keyspace, Some("ks".into()));
                assert_eq!(t.table, "t");
            }
            _ => panic!("expected Truncate"),
        }
    }

    #[test]
    fn parse_error_position() {
        let result = parse("SELECT * ???");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.position > 0);
    }

    #[test]
    fn parse_trailing_semicolon() {
        let stmt = parse("USE test;").unwrap();
        assert!(matches!(stmt, Statement::Use(_)));
    }

    #[test]
    fn parse_select_qualified_table() {
        let stmt = parse("SELECT * FROM system.local").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.keyspace, Some("system".into()));
                assert_eq!(s.table, "local");
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_allow_filtering() {
        let stmt = parse("SELECT * FROM t WHERE a = 1 ALLOW FILTERING").unwrap();
        match stmt {
            Statement::Select(s) => assert!(s.allow_filtering),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_insert_if_not_exists() {
        let stmt = parse("INSERT INTO t (id) VALUES (1) IF NOT EXISTS").unwrap();
        match stmt {
            Statement::Insert(i) => assert!(i.if_not_exists),
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_create_table_with_options() {
        let stmt = parse(
            "CREATE TABLE t (id int PRIMARY KEY) WITH gc_grace_seconds = 86400 AND comment = 'test'"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.options.get("gc_grace_seconds"), Some(&"86400".to_string()));
                assert_eq!(ct.options.get("comment"), Some(&"test".to_string()));
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_select_with_order_by() {
        let stmt = parse("SELECT * FROM t WHERE pk = 1 ORDER BY ck DESC").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.order_by.len(), 1);
                assert_eq!(s.order_by[0].1, ClusteringOrder::Desc);
            }
            _ => panic!("expected Select"),
        }
    }
}
