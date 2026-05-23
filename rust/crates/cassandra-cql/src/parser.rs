// Licensed under Apache License, Version 2.0.

//! CQL recursive-descent parser.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.CqlParser` (ANTLR-generated)
//!
//! ## Architecture
//! Hand-rolled recursive descent. Each parse_* method corresponds
//! to a grammar production. Errors include position information.

use crate::ast::*;
use crate::lexer::{Keyword, Lexer, Token, TokenKind};
use std::collections::HashMap;

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

/// Parse a standalone CQL term.
pub fn parse_term(input: &str) -> Result<Term, ParseError> {
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize().map_err(|e| ParseError {
        message: e.message,
        position: e.position,
    })?;
    let mut parser = Parser::new(tokens);
    let term = parser.parse_term()?;
    parser.expect_eof()?;
    Ok(term)
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

    fn expect_role_name(&mut self, allow_quoted_ident: bool) -> Result<String, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::Ident(s) | TokenKind::StringLiteral(s) => {
                self.advance();
                Ok(s)
            }
            TokenKind::QuotedIdent(s) if allow_quoted_ident => {
                self.advance();
                Ok(s)
            }
            TokenKind::QuotedIdent(_) => Err(self.error(
                "quoted identifiers are not supported for USER names; use ROLE".to_string(),
            )),
            TokenKind::Keyword(kw) => {
                if is_unreserved_keyword(kw) {
                    let name = format!("{:?}", kw).to_lowercase();
                    self.advance();
                    Ok(name)
                } else {
                    Err(self.error(format!("expected role name, got keyword {:?}", kw)))
                }
            }
            _ => Err(self.error(format!("expected role name, got {}", self.peek_kind()))),
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
            TokenKind::Keyword(Keyword::Begin) => {
                // Peek ahead: BEGIN TRANSACTION or BEGIN [UNLOGGED|COUNTER] BATCH
                let save = self.pos;
                self.advance(); // consume BEGIN
                if self.eat_keyword(Keyword::Transaction) {
                    self.parse_transaction_body()
                } else {
                    self.pos = save; // restore
                    self.parse_batch()
                }
            }
            TokenKind::Keyword(Keyword::Grant) => self.parse_grant(),
            TokenKind::Keyword(Keyword::Revoke) => self.parse_revoke(),
            TokenKind::Keyword(Keyword::List) => self.parse_list(),
            TokenKind::Keyword(Keyword::Describe) => self.parse_describe(),
            TokenKind::Keyword(Keyword::Comment) => self.parse_comment(),
            _ => Err(self.error(format!("unexpected token: {}", self.peek_kind()))),
        }
    }

    // ─── DDM ────────────────────────────────────────────────────────────

    fn parse_masked_with(&mut self) -> Result<Option<(String, Vec<Term>)>, ParseError> {
        if self.eat_keyword(Keyword::Masked) {
            self.expect_keyword(Keyword::With)?;
            let func_name = self.expect_ident()?;
            let mut args = Vec::new();
            if self.eat_if(TokenKind::LParen) {
                if *self.peek_kind() != TokenKind::RParen {
                    loop {
                        args.push(self.parse_term()?);
                        if !self.eat_if(TokenKind::Comma) {
                            break;
                        }
                    }
                }
                self.expect(TokenKind::RParen)?;
            }
            Ok(Some((func_name, args)))
        } else {
            Ok(None)
        }
    }

    fn parse_column_constraints(&mut self) -> Result<Vec<ColumnConstraint>, ParseError> {
        let mut constraints = Vec::new();
        if !self.eat_keyword(Keyword::Check) {
            return Ok(constraints);
        }

        loop {
            constraints.push(self.parse_column_constraint()?);
            if !self.eat_keyword(Keyword::And) {
                break;
            }
        }

        Ok(constraints)
    }

    fn parse_column_constraint(&mut self) -> Result<ColumnConstraint, ParseError> {
        if self.eat_keyword(Keyword::Not) {
            match self.peek_kind() {
                TokenKind::NullLiteral | TokenKind::Keyword(Keyword::Null) => {
                    self.advance();
                    return Ok(ColumnConstraint::NotNull);
                }
                _ => return Err(self.error("expected NULL after CHECK NOT".into())),
            }
        }

        let name = self.expect_ident()?;
        if self.eat_if(TokenKind::LParen) {
            let args = self.parse_constraint_args()?;
            self.expect(TokenKind::RParen)?;
            if self.is_constraint_relation_op() {
                let op = self.parse_constraint_relation_op()?;
                let term = self.parse_constraint_term()?;
                Ok(ColumnConstraint::Function {
                    name,
                    args,
                    op,
                    term,
                })
            } else {
                Ok(ColumnConstraint::UnaryFunction { name, args })
            }
        } else if self.is_constraint_relation_op() {
            let op = self.parse_constraint_relation_op()?;
            let term = self.parse_constraint_term()?;
            Ok(ColumnConstraint::Scalar {
                column: name,
                op,
                term,
            })
        } else {
            Ok(ColumnConstraint::UnaryFunction {
                name,
                args: Vec::new(),
            })
        }
    }

    fn parse_constraint_args(&mut self) -> Result<Vec<String>, ParseError> {
        let mut args = Vec::new();
        if *self.peek_kind() == TokenKind::RParen {
            return Ok(args);
        }

        loop {
            args.push(self.parse_constraint_arg()?);
            if !self.eat_if(TokenKind::Comma) {
                break;
            }
        }
        Ok(args)
    }

    fn parse_constraint_arg(&mut self) -> Result<String, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::StringLiteral(s) => {
                self.advance();
                Ok(s)
            }
            TokenKind::IntegerLiteral(i) => {
                self.advance();
                Ok(i.to_string())
            }
            TokenKind::FloatLiteral(f) => {
                self.advance();
                Ok(f.to_string())
            }
            TokenKind::BooleanLiteral(b) => {
                self.advance();
                Ok(b.to_string())
            }
            TokenKind::Ident(_) | TokenKind::QuotedIdent(_) | TokenKind::Keyword(_) => {
                self.expect_ident()
            }
            other => Err(self.error(format!("expected constraint argument, got {other}"))),
        }
    }

    fn is_constraint_relation_op(&self) -> bool {
        matches!(
            self.peek_kind(),
            TokenKind::Eq
                | TokenKind::Neq
                | TokenKind::Lt
                | TokenKind::Lte
                | TokenKind::Gt
                | TokenKind::Gte
        )
    }

    fn parse_constraint_relation_op(&mut self) -> Result<ConstraintRelationOp, ParseError> {
        let op = match self.peek_kind() {
            TokenKind::Eq => ConstraintRelationOp::Eq,
            TokenKind::Neq => ConstraintRelationOp::NotEq,
            TokenKind::Lt => ConstraintRelationOp::Lt,
            TokenKind::Lte => ConstraintRelationOp::Lte,
            TokenKind::Gt => ConstraintRelationOp::Gt,
            TokenKind::Gte => ConstraintRelationOp::Gte,
            other => return Err(self.error(format!("expected constraint operator, got {other}"))),
        };
        self.advance();
        Ok(op)
    }

    fn parse_constraint_term(&mut self) -> Result<String, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::StringLiteral(s) => {
                self.advance();
                Ok(s)
            }
            TokenKind::IntegerLiteral(i) => {
                self.advance();
                Ok(i.to_string())
            }
            TokenKind::FloatLiteral(f) => {
                self.advance();
                Ok(f.to_string())
            }
            TokenKind::BooleanLiteral(b) => {
                self.advance();
                Ok(b.to_string())
            }
            TokenKind::NullLiteral | TokenKind::Keyword(Keyword::Null) => {
                self.advance();
                Ok("NULL".to_string())
            }
            TokenKind::Ident(_) | TokenKind::QuotedIdent(_) | TokenKind::Keyword(_) => {
                self.expect_ident()
            }
            other => Err(self.error(format!("expected constraint term, got {other}"))),
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
        self.eat_ident_ci("COLUMNFAMILY");
        let (ks, name) = self.parse_table_name()?;
        Ok(Statement::Truncate(TruncateStatement {
            keyspace: ks,
            table: name,
        }))
    }

    // ─── CREATE ─────────────────────────────────────────────────────────

    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Create)?;
        // Handle CREATE OR REPLACE for functions/aggregates
        let or_replace = self.eat_keyword(Keyword::Or) && {
            self.expect_keyword(Keyword::Replace)?;
            true
        };
        match self.peek_kind().clone() {
            TokenKind::Keyword(Keyword::Keyspace) => self.parse_create_keyspace(),
            TokenKind::Keyword(Keyword::Table) => self.parse_create_table(),
            TokenKind::Keyword(Keyword::Index) | TokenKind::Keyword(Keyword::Custom) => self.parse_create_index(),
            TokenKind::Keyword(Keyword::Type) => self.parse_create_type(),
            TokenKind::Keyword(Keyword::Function) => self.parse_create_function(or_replace),
            TokenKind::Keyword(Keyword::Aggregate) => self.parse_create_aggregate(or_replace),
            TokenKind::Keyword(Keyword::Trigger) => self.parse_create_trigger(),
            TokenKind::Keyword(Keyword::Role) => self.parse_create_role(false),
            TokenKind::Keyword(Keyword::User) => self.parse_create_role(true),
            TokenKind::Keyword(Keyword::Materialized) => self.parse_create_materialized_view(),
            _ => Err(self.error(format!(
                "expected KEYSPACE, TABLE, INDEX, TYPE, FUNCTION, AGGREGATE, TRIGGER, ROLE, USER, or MATERIALIZED after CREATE, got {}",
                self.peek_kind()
            ))),
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
                let masked_with = self.parse_masked_with()?;
                let constraints = self.parse_column_constraints()?;
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
                    masked_with,
                    constraints,
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
                    if !self.eat_keyword(Keyword::And) {
                        break;
                    }
                }
                Ok(Statement::AlterKeyspace(AlterKeyspace {
                    name,
                    replication,
                    durable_writes,
                }))
            }
            TokenKind::Keyword(Keyword::Table) => {
                self.expect_keyword(Keyword::Table)?;
                let (ks, name) = self.parse_table_name()?;
                let operation = if self.eat_keyword(Keyword::Add) {
                    let col_name = self.expect_ident()?;
                    let cql_type = self.parse_cql_type()?;
                    let is_static = self.eat_keyword(Keyword::Static);
                    let masked_with = self.parse_masked_with()?;
                    let constraints = self.parse_column_constraints()?;
                    AlterTableOp::AddColumn(ColumnDef {
                        name: col_name,
                        cql_type,
                        is_static,
                        masked_with,
                        constraints,
                    })
                } else if self.eat_keyword(Keyword::Alter) {
                    self.eat_keyword(Keyword::Column); // Optional COLUMN keyword
                    let col_name = self.expect_ident()?;
                    if self.eat_keyword(Keyword::Drop) {
                        if self.eat_keyword(Keyword::Masked) {
                            AlterTableOp::DropMask(col_name)
                        } else {
                            self.expect_keyword(Keyword::Check)?;
                            AlterTableOp::DropConstraints(col_name)
                        }
                    } else if let Some((func, args)) = self.parse_masked_with()? {
                        AlterTableOp::MaskColumn(col_name, func, args)
                    } else if *self.peek_kind() == TokenKind::Keyword(Keyword::Check) {
                        AlterTableOp::AlterConstraints(col_name, self.parse_column_constraints()?)
                    } else if self.eat_keyword(Keyword::Type) {
                        let cql_type = self.parse_cql_type()?;
                        AlterTableOp::AlterColumn(col_name, cql_type)
                    } else {
                        // Fallback type alter without TYPE
                        let cql_type = self.parse_cql_type()?;
                        AlterTableOp::AlterColumn(col_name, cql_type)
                    }
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
                        if !self.eat_keyword(Keyword::And) {
                            break;
                        }
                    }
                    AlterTableOp::WithOptions(opts)
                } else {
                    return Err(
                        self.error("expected ADD, ALTER, DROP, or WITH after ALTER TABLE".into())
                    );
                };
                Ok(Statement::AlterTable(AlterTable {
                    keyspace: ks,
                    name,
                    operation,
                }))
            }
            TokenKind::Keyword(Keyword::Role) | TokenKind::Keyword(Keyword::User) => {
                let is_user = self.eat_keyword(Keyword::User);
                if !is_user {
                    self.expect_keyword(Keyword::Role)?;
                }
                let if_exists = self.parse_if_exists();
                let name = self.expect_role_name(!is_user)?;
                let mut password = None;
                let mut hashed_password = None;
                let mut superuser = None;
                let mut login = None;
                let mut datacenter_access = None;
                let mut cidr_access = None;
                let mut options = HashMap::new();
                if self.eat_keyword(Keyword::With) {
                    loop {
                        if self.eat_keyword(Keyword::Hashed) {
                            self.expect_keyword(Keyword::Password)?;
                            self.parse_password_equals(is_user)?;
                            hashed_password = Some(self.parse_string_literal()?);
                        } else if self.eat_keyword(Keyword::Password) {
                            self.parse_password_equals(is_user)?;
                            password = Some(self.parse_string_literal()?);
                        } else if self.eat_keyword(Keyword::Superuser) {
                            self.expect(TokenKind::Eq)?;
                            superuser = Some(self.parse_boolean()?);
                        } else if self.eat_keyword(Keyword::Login) {
                            self.expect(TokenKind::Eq)?;
                            login = Some(self.parse_boolean()?);
                        } else if self.eat_ident_ci("ACCESS") {
                            self.parse_role_access_option(
                                &mut datacenter_access,
                                &mut cidr_access,
                            )?;
                        } else if self.eat_ident_ci("OPTIONS") {
                            options = self.parse_role_options()?;
                        } else {
                            break;
                        }
                        if !self.eat_keyword(Keyword::And) {
                            break;
                        }
                    }
                }
                Ok(Statement::AlterRole(AlterRole {
                    name,
                    if_exists,
                    password,
                    hashed_password,
                    superuser,
                    login,
                    datacenter_access,
                    cidr_access,
                    options,
                }))
            }
            TokenKind::Keyword(Keyword::Type) => {
                self.expect_keyword(Keyword::Type)?;
                let (ks, name) = self.parse_table_name()?;
                let operation = if self.eat_keyword(Keyword::Add) {
                    let field_name = self.expect_ident()?;
                    let field_type = self.parse_cql_type()?;
                    AlterTypeOp::AddField(field_name, field_type)
                } else if self.eat_keyword(Keyword::Rename) {
                    let from = self.expect_ident()?;
                    // Expect "TO" as an identifier since it's not a keyword
                    let to_kw = self.expect_ident()?;
                    if !to_kw.eq_ignore_ascii_case("to") {
                        return Err(self.error("expected TO after RENAME field name".into()));
                    }
                    let to = self.expect_ident()?;
                    AlterTypeOp::RenameField(from, to)
                } else if self.eat_keyword(Keyword::Alter) {
                    let field_name = self.expect_ident()?;
                    self.expect_keyword(Keyword::Type)?;
                    let field_type = self.parse_cql_type()?;
                    AlterTypeOp::AlterFieldType(field_name, field_type)
                } else {
                    return Err(
                        self.error("expected ADD, RENAME, or ALTER after ALTER TYPE <name>".into())
                    );
                };
                Ok(Statement::AlterType(AlterType {
                    keyspace: ks,
                    name,
                    operation,
                }))
            }
            TokenKind::Keyword(Keyword::Materialized) => {
                self.expect_keyword(Keyword::Materialized)?;
                self.expect_keyword(Keyword::View)?;
                let (ks, name) = self.parse_table_name()?;
                self.expect_keyword(Keyword::With)?;
                let mut opts = HashMap::new();
                loop {
                    let key = self.expect_ident()?;
                    self.expect(TokenKind::Eq)?;
                    let val = self.parse_option_value()?;
                    opts.insert(key, val);
                    if !self.eat_keyword(Keyword::And) {
                        break;
                    }
                }
                Ok(Statement::AlterMaterializedView(AlterMaterializedView {
                    keyspace: ks,
                    name,
                    options: opts,
                }))
            }
            _ => Err(self.error(
                "expected KEYSPACE, TABLE, ROLE, USER, TYPE, or MATERIALIZED after ALTER".into(),
            )),
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
            TokenKind::Keyword(Keyword::Index) => {
                self.expect_keyword(Keyword::Index)?;
                let if_exists = self.parse_if_exists();
                let (ks, name) = self.parse_table_name()?;
                Ok(Statement::DropIndex(DropIndex { keyspace: ks, name, if_exists }))
            }
            TokenKind::Keyword(Keyword::Type) => {
                self.expect_keyword(Keyword::Type)?;
                let if_exists = self.parse_if_exists();
                let (ks, name) = self.parse_table_name()?;
                Ok(Statement::DropType(DropType { keyspace: ks, name, if_exists }))
            }
            TokenKind::Keyword(Keyword::Function) => {
                self.expect_keyword(Keyword::Function)?;
                let if_exists = self.parse_if_exists();
                let (ks, name) = self.parse_table_name()?;
                let arg_types = if self.eat_if(TokenKind::LParen) {
                    let mut types = Vec::new();
                    if *self.peek_kind() != TokenKind::RParen {
                        loop {
                            types.push(self.parse_cql_type()?);
                            if !self.eat_if(TokenKind::Comma) { break; }
                        }
                    }
                    self.expect(TokenKind::RParen)?;
                    types
                } else { Vec::new() };
                Ok(Statement::DropFunction(DropFunction { keyspace: ks, name, if_exists, arg_types }))
            }
            TokenKind::Keyword(Keyword::Aggregate) => {
                self.expect_keyword(Keyword::Aggregate)?;
                let if_exists = self.parse_if_exists();
                let (ks, name) = self.parse_table_name()?;
                let arg_types = if self.eat_if(TokenKind::LParen) {
                    let mut types = Vec::new();
                    if *self.peek_kind() != TokenKind::RParen {
                        loop {
                            types.push(self.parse_cql_type()?);
                            if !self.eat_if(TokenKind::Comma) { break; }
                        }
                    }
                    self.expect(TokenKind::RParen)?;
                    types
                } else { Vec::new() };
                Ok(Statement::DropAggregate(DropAggregate { keyspace: ks, name, if_exists, arg_types }))
            }
            TokenKind::Keyword(Keyword::Trigger) => {
                self.expect_keyword(Keyword::Trigger)?;
                let if_exists = self.parse_if_exists();
                let name = self.expect_ident()?;
                self.expect_keyword(Keyword::On)?;
                let (ks, table) = self.parse_table_name()?;
                Ok(Statement::DropTrigger(DropTrigger { name, if_exists, keyspace: ks, table }))
            }
            TokenKind::Keyword(Keyword::Role) | TokenKind::Keyword(Keyword::User) => {
                let is_user = self.eat_keyword(Keyword::User);
                if !is_user {
                    self.expect_keyword(Keyword::Role)?;
                }
                let if_exists = self.parse_if_exists();
                let name = self.expect_role_name(!is_user)?;
                Ok(Statement::DropRole(DropRole { name, if_exists }))
            }
            TokenKind::Keyword(Keyword::Materialized) => {
                self.expect_keyword(Keyword::Materialized)?;
                self.expect_keyword(Keyword::View)?;
                let if_exists = self.parse_if_exists();
                let (ks, name) = self.parse_table_name()?;
                Ok(Statement::DropMaterializedView(DropMaterializedView { keyspace: ks, name, if_exists }))
            }
            _ => Err(self.error("expected KEYSPACE, TABLE, INDEX, TYPE, FUNCTION, AGGREGATE, TRIGGER, ROLE, USER, or MATERIALIZED after DROP".into())),
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

        let mut group_by = Vec::new();
        if self.eat_keyword(Keyword::Group) {
            self.expect_keyword(Keyword::By)?;
            loop {
                group_by.push(self.expect_ident()?);
                if !self.eat_if(TokenKind::Comma) {
                    break;
                }
            }
        }

        let mut order_by = Vec::new();
        let mut ann_order_by = None;
        if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            loop {
                let col = self.expect_ident()?;
                if self.eat_ident_ci("ANN") {
                    self.expect_keyword(Keyword::Of)?;
                    let vector_literal = self.parse_ann_vector_literal()?;
                    ann_order_by = Some(SelectAnnOrder {
                        column: col,
                        vector_literal,
                    });
                } else {
                    let order = if self.eat_keyword(Keyword::Desc) {
                        ClusteringOrder::Desc
                    } else {
                        self.eat_keyword(Keyword::Asc);
                        ClusteringOrder::Asc
                    };
                    order_by.push((col, order));
                }
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
            group_by,
            order_by,
            ann_order_by,
            limit,
            per_partition_limit,
            allow_filtering,
        }))
    }

    fn parse_selector(&mut self) -> Result<Selector, ParseError> {
        self.parse_selector_allow_alias(true)
    }

    fn parse_selector_allow_alias(&mut self, allow_alias: bool) -> Result<Selector, ParseError> {
        let name = self.expect_ident()?;
        // Check for function call.
        if self.eat_if(TokenKind::LParen) {
            if *self.peek_kind() == TokenKind::Star {
                self.advance();
                self.expect(TokenKind::RParen)?;
                if allow_alias && self.eat_keyword(Keyword::As) {
                    let alias = self.expect_ident()?;
                    return Ok(Selector::Alias {
                        selector: Box::new(Selector::Count),
                        alias,
                    });
                }
                return Ok(Selector::Count);
            }
            if name.eq_ignore_ascii_case("count")
                && matches!(self.peek_kind(), TokenKind::IntegerLiteral(1))
            {
                self.advance();
                self.expect(TokenKind::RParen)?;
                if allow_alias && self.eat_keyword(Keyword::As) {
                    let alias = self.expect_ident()?;
                    return Ok(Selector::Alias {
                        selector: Box::new(Selector::Count),
                        alias,
                    });
                }
                return Ok(Selector::Count);
            }
            if name.eq_ignore_ascii_case("cast") {
                let selector = self.parse_selector_allow_alias(false)?;
                self.expect_keyword(Keyword::As)?;
                let target = self.parse_cql_type()?;
                self.expect(TokenKind::RParen)?;
                let sel = Selector::Cast {
                    selector: Box::new(selector),
                    target,
                };
                if allow_alias && self.eat_keyword(Keyword::As) {
                    let alias = self.expect_ident()?;
                    return Ok(Selector::Alias {
                        selector: Box::new(sel),
                        alias,
                    });
                }
                return Ok(sel);
            }
            let mut args = Vec::new();
            if *self.peek_kind() != TokenKind::RParen {
                loop {
                    args.push(self.parse_selector_allow_alias(false)?);
                    if !self.eat_if(TokenKind::Comma) {
                        break;
                    }
                }
            }
            self.expect(TokenKind::RParen)?;
            let sel = if is_writetime_or_ttl_selector(&name) {
                match args.as_slice() {
                    [Selector::Column(column)] => {
                        Selector::WritetimeOrTtl(name.to_ascii_lowercase(), column.clone())
                    }
                    _ => {
                        return Err(
                            self.error(format!("{} selector expects exactly one column", name))
                        );
                    }
                }
            } else {
                Selector::Function(name, args)
            };
            if allow_alias && self.eat_keyword(Keyword::As) {
                let alias = self.expect_ident()?;
                return Ok(Selector::Alias {
                    selector: Box::new(sel),
                    alias,
                });
            }
            return Ok(sel);
        }
        if allow_alias && self.eat_keyword(Keyword::As) {
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
            let json_default = self.parse_json_default()?;
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
                json_default,
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
            json_default: JsonDefault::Null,
            using,
        }))
    }

    fn parse_json_default(&mut self) -> Result<JsonDefault, ParseError> {
        if !self.eat_keyword(Keyword::Default) {
            return Ok(JsonDefault::Null);
        }
        if self.eat_keyword(Keyword::Null) {
            return Ok(JsonDefault::Null);
        }
        if self.eat_keyword(Keyword::Unset) {
            return Ok(JsonDefault::Unset);
        }
        Err(self.error("expected NULL or UNSET after INSERT JSON DEFAULT".into()))
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
            let (op, val) = if self.eat_if(TokenKind::LBracket) {
                let key = self.parse_term()?;
                self.expect(TokenKind::RBracket)?;
                self.expect(TokenKind::Eq)?;
                (AssignmentOp::MapPut { key }, self.parse_term()?)
            } else {
                self.expect(TokenKind::Eq)?;
                self.parse_assignment_value(&col)?
            };
            assignments.push(Assignment {
                column: col,
                value: val,
                op,
            });
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
        let using = self.parse_delete_using()?;

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

    fn parse_delete_using(&mut self) -> Result<Vec<UsingClause>, ParseError> {
        if !self.eat_keyword(Keyword::Using) {
            return Ok(Vec::new());
        }
        self.expect_keyword(Keyword::Timestamp)?;
        Ok(vec![UsingClause::Timestamp(self.parse_term()?)])
    }

    // ─── BATCH ──────────────────────────────────────────────────────────

    /// Parse the body of a BEGIN TRANSACTION ... COMMIT TRANSACTION statement.
    fn parse_transaction_body(&mut self) -> Result<Statement, ParseError> {
        let mut let_bindings = Vec::new();
        let mut statements = Vec::new();

        // Parse LET bindings and DML statements until COMMIT
        loop {
            if self.eat_keyword(Keyword::Commit) {
                self.expect_keyword(Keyword::Transaction)?;
                break;
            }

            if self.eat_keyword(Keyword::Let) {
                // LET <name> = (<select>)
                let name = self.expect_ident()?;
                self.expect(TokenKind::Eq)?;
                self.expect(TokenKind::LParen)?;
                // Parse the inner SELECT
                let select_stmt = self.parse_select()?;
                let select = match select_stmt {
                    Statement::Select(s) => s,
                    _ => return Err(self.error("LET binding must contain a SELECT".to_string())),
                };
                self.expect(TokenKind::RParen)?;
                self.eat_if(TokenKind::Semicolon);
                let_bindings.push(LetBinding { name, select });
            } else {
                // Parse a DML statement (INSERT, UPDATE, DELETE)
                let stmt = self.parse_statement()?;
                match &stmt {
                    Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {}
                    _ => {
                        return Err(self.error(
                            "Only INSERT, UPDATE, DELETE allowed in transactions".to_string(),
                        ));
                    }
                }
                self.eat_if(TokenKind::Semicolon);
                statements.push(stmt);
            }
        }

        // Optional RETURNING clause
        let returning = if self.eat_keyword(Keyword::Returning) {
            let mut columns = Vec::new();
            loop {
                let sel = self.parse_selector()?;
                columns.push(sel);
                if !self.eat_if(TokenKind::Comma) {
                    break;
                }
            }
            Some(ReturningClause { columns })
        } else {
            None
        };

        Ok(Statement::Transaction(TransactionStatement {
            let_bindings,
            statements,
            returning,
        }))
    }

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
            let stmt = self.parse_batch_statement_objective()?;
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

    fn parse_batch_statement_objective(&mut self) -> Result<Statement, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::Keyword(Keyword::Insert) => self.parse_insert(),
            TokenKind::Keyword(Keyword::Update) => self.parse_update(),
            TokenKind::Keyword(Keyword::Delete) => self.parse_delete(),
            _ => Err(self.error(format!(
                "expected INSERT, UPDATE, DELETE, or APPLY in BATCH, got {}",
                self.peek_kind()
            ))),
        }
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

    fn parse_dotted_name(&mut self) -> Result<Vec<String>, ParseError> {
        let mut parts = vec![self.expect_ident()?];
        while self.eat_if(TokenKind::Dot) {
            parts.push(self.expect_ident()?);
        }
        Ok(parts)
    }

    fn parse_if_not_exists(&mut self) -> bool {
        if self.eat_keyword(Keyword::If) && self.eat_keyword(Keyword::Not) {
            let _ = self.expect_keyword(Keyword::Exists);
            return true;
        }
        false
    }

    fn parse_if_exists(&mut self) -> bool {
        if self.eat_keyword(Keyword::If) && self.eat_keyword(Keyword::Exists) {
            return true;
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
            let (col, collection_key) = if self.eat_if(TokenKind::LParen) {
                let columns = self.parse_ident_list()?;
                self.expect(TokenKind::RParen)?;
                (format!("({})", columns.join(",")), None)
            } else {
                let ident = self.expect_ident()?;
                if ident.eq_ignore_ascii_case("token") && self.eat_if(TokenKind::LParen) {
                    if *self.peek_kind() != TokenKind::RParen {
                        self.parse_ident_list()?;
                    }
                    self.expect(TokenKind::RParen)?;
                    ("token".to_string(), None)
                } else {
                    let collection_key = if self.eat_if(TokenKind::LBracket) {
                        let key = self.parse_term()?;
                        self.expect(TokenKind::RBracket)?;
                        Some(key)
                    } else {
                        None
                    };
                    (ident, collection_key)
                }
            };
            let op = self.parse_relation_op()?;
            let mut value = self.parse_term()?;
            if let Some(key) = collection_key {
                value = Term::CollectionElement {
                    key: Box::new(key),
                    value: Box::new(value),
                };
            }
            relations.push(Relation {
                column: col,
                op,
                value,
            });
            if !self.eat_keyword(Keyword::And) {
                break;
            }
        }
        Ok(relations)
    }

    fn parse_assignment_value(&mut self, column: &str) -> Result<(AssignmentOp, Term), ParseError> {
        let left = self.parse_term()?;
        if self.eat_if(TokenKind::Plus) {
            let right = self.parse_term()?;
            if is_column_reference(&left, column) {
                Ok((AssignmentOp::CollectionAppend, right))
            } else if is_column_reference(&right, column) {
                Ok((AssignmentOp::CollectionPrepend, left))
            } else {
                Err(self.error(format!(
                    "collection append/prepend for '{}' must reference the assigned column",
                    column
                )))
            }
        } else if self.eat_if(TokenKind::Minus) {
            let right = self.parse_term()?;
            if is_column_reference(&left, column) {
                Ok((AssignmentOp::CollectionRemove, right))
            } else {
                Err(self.error(format!(
                    "collection removal for '{}' must reference the assigned column",
                    column
                )))
            }
        } else {
            Ok((AssignmentOp::Set, left))
        }
    }

    fn parse_relation_op(&mut self) -> Result<RelationOp, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::Eq => {
                self.advance();
                Ok(RelationOp::Eq)
            }
            TokenKind::Neq => {
                self.advance();
                Ok(RelationOp::Neq)
            }
            TokenKind::Lt => {
                self.advance();
                Ok(RelationOp::Lt)
            }
            TokenKind::Gt => {
                self.advance();
                Ok(RelationOp::Gt)
            }
            TokenKind::Lte => {
                self.advance();
                Ok(RelationOp::Lte)
            }
            TokenKind::Gte => {
                self.advance();
                Ok(RelationOp::Gte)
            }
            TokenKind::Keyword(Keyword::In) => {
                self.advance();
                Ok(RelationOp::In)
            }
            TokenKind::Keyword(Keyword::Contains) => {
                self.advance();
                if self.eat_keyword(Keyword::Key) {
                    Ok(RelationOp::ContainsKey)
                } else {
                    Ok(RelationOp::Contains)
                }
            }
            TokenKind::Keyword(Keyword::Like) => {
                self.advance();
                Ok(RelationOp::Like)
            }
            _ => Err(self.error(format!(
                "expected comparison operator, got {}",
                self.peek_kind()
            ))),
        }
    }

    fn eat_ident_ci(&mut self, expected: &str) -> bool {
        match self.peek_kind() {
            TokenKind::Ident(actual) if actual.eq_ignore_ascii_case(expected) => {
                self.advance();
                true
            }
            _ => false,
        }
    }

    fn expect_ident_ci(&mut self, expected: &str) -> Result<(), ParseError> {
        if self.eat_ident_ci(expected) {
            Ok(())
        } else {
            Err(self.error(format!("expected {expected}, got {}", self.peek_kind())))
        }
    }

    fn parse_ann_vector_literal(&mut self) -> Result<Vec<f32>, ParseError> {
        self.expect(TokenKind::LBracket)?;
        let mut values = Vec::new();
        if *self.peek_kind() != TokenKind::RBracket {
            loop {
                values.push(self.parse_f32_literal()?);
                if !self.eat_if(TokenKind::Comma) {
                    break;
                }
            }
        }
        self.expect(TokenKind::RBracket)?;
        if values.is_empty() {
            return Err(self.error("ANN vector literal requires at least one value".into()));
        }
        Ok(values)
    }

    fn parse_f32_literal(&mut self) -> Result<f32, ParseError> {
        let negative = self.eat_if(TokenKind::Minus);
        let value = match self.peek_kind().clone() {
            TokenKind::IntegerLiteral(value) => {
                self.advance();
                value as f32
            }
            TokenKind::FloatLiteral(value) => {
                self.advance();
                value as f32
            }
            other => {
                return Err(self.error(format!(
                    "expected numeric vector literal value, got {}",
                    other
                )));
            }
        };
        Ok(if negative { -value } else { value })
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
                let start = self.pos;
                self.advance();
                if let Ok(cql_type) = self.parse_cql_type() {
                    if self.eat_if(TokenKind::RParen) && is_term_start(self.peek_kind()) {
                        let inner = self.parse_term()?;
                        return Ok(Term::TypeHint(cql_type, Box::new(inner)));
                    }
                }

                self.pos = start;
                self.expect(TokenKind::LParen)?;
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
                if *self.peek_kind() == TokenKind::RBrace {
                    self.expect(TokenKind::RBrace)?;
                    return Ok(Term::MapLiteral(Vec::new()));
                }

                let first = self.parse_term()?;
                if self.eat_if(TokenKind::Colon) {
                    let mut entries = vec![(first, self.parse_term()?)];
                    while self.eat_if(TokenKind::Comma) {
                        let key = self.parse_term()?;
                        self.expect(TokenKind::Colon)?;
                        let val = self.parse_term()?;
                        entries.push((key, val));
                    }
                    self.expect(TokenKind::RBrace)?;
                    return Ok(Term::MapLiteral(entries));
                }

                let mut items = vec![first];
                while self.eat_if(TokenKind::Comma) {
                    items.push(self.parse_term()?);
                }
                self.expect(TokenKind::RBrace)?;
                Ok(Term::CollectionLiteral(items))
            }
            TokenKind::Keyword(Keyword::Cast) => self.parse_cast_term(),
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
            _ => Err(self.error(format!(
                "expected value or bind marker, got {}",
                self.peek_kind()
            ))),
        }
    }

    fn parse_cast_term(&mut self) -> Result<Term, ParseError> {
        self.expect_keyword(Keyword::Cast)?;
        self.expect(TokenKind::LParen)?;
        let inner = self.parse_term()?;
        self.expect_keyword(Keyword::As)?;
        let target = self.parse_cql_type()?;
        self.expect(TokenKind::RParen)?;
        Ok(Term::TypeHint(target, Box::new(inner)))
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

        if self.eat_keyword(Keyword::Tuple) {
            self.expect(TokenKind::Lt)?;
            if *self.peek_kind() == TokenKind::Gt {
                return Err(self.error("tuple type requires at least one field".into()));
            }

            let mut field_types = Vec::new();
            loop {
                field_types.push(self.parse_cql_type()?);
                if !self.eat_if(TokenKind::Comma) {
                    break;
                }
            }
            self.expect(TokenKind::Gt)?;
            return Ok(CqlTypeName::Tuple(field_types));
        }

        if self.eat_keyword(Keyword::Vector) {
            self.expect(TokenKind::Lt)?;
            let inner = self.parse_cql_type()?;
            self.expect(TokenKind::Comma)?;
            let dimensions = match self.peek_kind() {
                TokenKind::IntegerLiteral(value) if *value > 0 => {
                    let dimensions = u32::try_from(*value)
                        .map_err(|_| self.error("vector dimension is too large".into()))?;
                    self.advance();
                    dimensions
                }
                TokenKind::IntegerLiteral(_) => {
                    return Err(self.error("vector dimension must be greater than zero".into()));
                }
                _ => return Err(self.error("expected positive vector dimension".into())),
            };
            self.expect(TokenKind::Gt)?;
            return Ok(CqlTypeName::Vector(Box::new(inner), dimensions));
        }

        let name = self.expect_ident()?;
        Ok(CqlTypeName::Simple(name))
    }

    fn parse_map_literal_strings(&mut self) -> Result<HashMap<String, String>, ParseError> {
        self.expect(TokenKind::LBrace)?;
        let mut map = HashMap::new();
        if *self.peek_kind() != TokenKind::RBrace {
            loop {
                let key = match self.peek_kind().clone() {
                    TokenKind::StringLiteral(s) => {
                        self.advance();
                        s
                    }
                    _ => return Err(self.error("expected string key in map".into())),
                };
                self.expect(TokenKind::Colon)?;
                let value = match self.peek_kind().clone() {
                    TokenKind::StringLiteral(s) => {
                        self.advance();
                        s
                    }
                    TokenKind::IntegerLiteral(n) => {
                        self.advance();
                        n.to_string()
                    }
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

    fn parse_string_set(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect(TokenKind::LBrace)?;
        let mut values = Vec::new();
        if *self.peek_kind() != TokenKind::RBrace {
            loop {
                values.push(self.parse_string_literal()?);
                if !self.eat_if(TokenKind::Comma) {
                    break;
                }
            }
        }
        self.expect(TokenKind::RBrace)?;
        Ok(values)
    }

    fn parse_option_value(&mut self) -> Result<String, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::StringLiteral(s) => {
                self.advance();
                Ok(s)
            }
            TokenKind::IntegerLiteral(n) => {
                self.advance();
                Ok(n.to_string())
            }
            TokenKind::FloatLiteral(f) => {
                self.advance();
                Ok(f.to_string())
            }
            TokenKind::BooleanLiteral(b) => {
                self.advance();
                Ok(b.to_string())
            }
            TokenKind::LBrace => {
                let map = self.parse_map_literal_strings()?;
                serde_json::to_string(&map)
                    .map_err(|err| self.error(format!("invalid option map: {err}")))
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

    // ─── CREATE INDEX ────────────────────────────────────────────────────

    fn parse_create_index(&mut self) -> Result<Statement, ParseError> {
        let custom = self.eat_keyword(Keyword::Custom);
        self.expect_keyword(Keyword::Index)?;
        let if_not_exists = self.parse_if_not_exists();
        let name = if *self.peek_kind() != TokenKind::Keyword(Keyword::On) {
            Some(self.expect_ident()?)
        } else {
            None
        };
        self.expect_keyword(Keyword::On)?;
        let (ks, table) = self.parse_table_name()?;
        self.expect(TokenKind::LParen)?;
        let column = self.expect_ident()?;
        self.expect(TokenKind::RParen)?;
        let custom_class = if custom {
            if self.eat_keyword(Keyword::Using) {
                Some(self.parse_string_literal()?)
            } else {
                None
            }
        } else {
            None
        };
        let options = if self.eat_keyword(Keyword::With) {
            let options_kw = self.expect_ident()?;
            if !options_kw.eq_ignore_ascii_case("options") {
                return Err(self.error("expected OPTIONS after WITH in CREATE INDEX".into()));
            }
            self.expect(TokenKind::Eq)?;
            self.parse_map_literal_strings()?
        } else {
            HashMap::new()
        };
        Ok(Statement::CreateIndex(CreateIndex {
            name,
            if_not_exists,
            keyspace: ks,
            table,
            column,
            index_target: None,
            custom_class,
            options,
        }))
    }

    // ─── CREATE TYPE ────────────────────────────────────────────────────

    fn parse_create_type(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Type)?;
        let if_not_exists = self.parse_if_not_exists();
        let (ks, name) = self.parse_table_name()?;
        self.expect(TokenKind::LParen)?;
        let mut fields = Vec::new();
        loop {
            let fname = self.expect_ident()?;
            let ftype = self.parse_cql_type()?;
            fields.push((fname, ftype));
            if !self.eat_if(TokenKind::Comma) {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        Ok(Statement::CreateType(CreateType {
            keyspace: ks,
            name,
            if_not_exists,
            fields,
        }))
    }

    // ─── CREATE FUNCTION ────────────────────────────────────────────────

    fn parse_create_function(&mut self, or_replace: bool) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Function)?;
        let if_not_exists = self.parse_if_not_exists();
        let (ks, name) = self.parse_table_name()?;
        self.expect(TokenKind::LParen)?;
        let mut args = Vec::new();
        if *self.peek_kind() != TokenKind::RParen {
            loop {
                let arg_name = self.expect_ident()?;
                let arg_type = self.parse_cql_type()?;
                args.push((arg_name, arg_type));
                if !self.eat_if(TokenKind::Comma) {
                    break;
                }
            }
        }
        self.expect(TokenKind::RParen)?;
        // CALLED ON NULL INPUT or RETURNS NULL ON NULL INPUT
        let called_on_null_input = if self.eat_keyword(Keyword::Called) {
            self.expect_keyword(Keyword::On)?;
            // skip NULL INPUT
            self.advance(); // NULL
            self.advance(); // INPUT
            true
        } else if self.eat_keyword(Keyword::Returns) {
            // RETURNS NULL ON NULL INPUT
            if self.eat_keyword(Keyword::Null) {
                self.expect_keyword(Keyword::On)?;
                self.advance(); // NULL
                self.advance(); // INPUT
                // Now parse the real RETURNS
                self.expect_keyword(Keyword::Returns)?;
                false
            } else {
                // Just RETURNS <type>
                false
            }
        } else {
            false
        };
        // If we haven't parsed RETURNS yet:
        if !called_on_null_input && *self.peek_kind() != TokenKind::Keyword(Keyword::Returns) {
            // already consumed RETURNS above
        } else if called_on_null_input {
            self.expect_keyword(Keyword::Returns)?;
        }
        let return_type = self.parse_cql_type()?;
        self.expect_keyword(Keyword::Language)?;
        let language = self.expect_ident()?;
        self.expect_keyword(Keyword::As)?;
        let body = self.parse_string_literal()?;
        Ok(Statement::CreateFunction(CreateFunction {
            keyspace: ks,
            name,
            or_replace,
            if_not_exists,
            args,
            called_on_null_input,
            return_type,
            language,
            body,
        }))
    }

    // ─── CREATE AGGREGATE ───────────────────────────────────────────────

    fn parse_create_aggregate(&mut self, or_replace: bool) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Aggregate)?;
        let if_not_exists = self.parse_if_not_exists();
        let (ks, name) = self.parse_table_name()?;
        self.expect(TokenKind::LParen)?;
        let mut arg_types = Vec::new();
        if *self.peek_kind() != TokenKind::RParen {
            loop {
                arg_types.push(self.parse_cql_type()?);
                if !self.eat_if(TokenKind::Comma) {
                    break;
                }
            }
        }
        self.expect(TokenKind::RParen)?;
        self.expect_keyword(Keyword::Sfunc)?;
        let sfunc = self.expect_ident()?;
        self.expect_keyword(Keyword::Stype)?;
        let stype = self.parse_cql_type()?;
        let finalfunc = if self.eat_keyword(Keyword::Finalfunc) {
            Some(self.expect_ident()?)
        } else {
            None
        };
        let initcond = if self.eat_keyword(Keyword::Initcond) {
            Some(self.parse_term()?)
        } else {
            None
        };
        Ok(Statement::CreateAggregate(CreateAggregate {
            keyspace: ks,
            name,
            or_replace,
            if_not_exists,
            arg_types,
            sfunc,
            stype,
            finalfunc,
            initcond,
        }))
    }

    // ─── CREATE TRIGGER ─────────────────────────────────────────────────

    fn parse_create_trigger(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Trigger)?;
        let if_not_exists = self.parse_if_not_exists();
        let name = self.expect_ident()?;
        self.expect_keyword(Keyword::On)?;
        let (ks, table) = self.parse_table_name()?;
        self.expect_keyword(Keyword::Using)?;
        let trigger_class = self.parse_string_literal()?;
        Ok(Statement::CreateTrigger(CreateTrigger {
            name,
            if_not_exists,
            keyspace: ks,
            table,
            trigger_class,
        }))
    }

    // ─── CREATE ROLE ────────────────────────────────────────────────────

    fn parse_create_role(&mut self, is_user: bool) -> Result<Statement, ParseError> {
        if is_user {
            self.expect_keyword(Keyword::User)?;
        } else {
            self.expect_keyword(Keyword::Role)?;
        }
        let if_not_exists = self.parse_if_not_exists();
        let name = self.expect_role_name(!is_user)?;
        let mut password = None;
        let mut hashed_password = None;
        let mut superuser = None;
        let mut login = if is_user { Some(true) } else { None };
        let mut datacenter_access = None;
        let mut cidr_access = None;
        let mut options = HashMap::new();
        if self.eat_keyword(Keyword::With) {
            loop {
                if self.eat_keyword(Keyword::Hashed) {
                    self.expect_keyword(Keyword::Password)?;
                    self.parse_password_equals(is_user)?;
                    hashed_password = Some(self.parse_string_literal()?);
                } else if self.eat_keyword(Keyword::Password) {
                    self.parse_password_equals(is_user)?;
                    password = Some(self.parse_string_literal()?);
                } else if self.eat_keyword(Keyword::Superuser) {
                    self.expect(TokenKind::Eq)?;
                    superuser = Some(self.parse_boolean()?);
                } else if self.eat_keyword(Keyword::Login) {
                    self.expect(TokenKind::Eq)?;
                    login = Some(self.parse_boolean()?);
                } else if self.eat_ident_ci("ACCESS") {
                    self.parse_role_access_option(&mut datacenter_access, &mut cidr_access)?;
                } else if self.eat_ident_ci("OPTIONS") {
                    options = self.parse_role_options()?;
                } else {
                    break;
                }
                if !self.eat_keyword(Keyword::And) {
                    break;
                }
            }
        }
        Ok(Statement::CreateRole(CreateRole {
            name,
            if_not_exists,
            password,
            hashed_password,
            superuser,
            login,
            datacenter_access,
            cidr_access,
            options,
        }))
    }

    fn parse_password_equals(&mut self, optional: bool) -> Result<(), ParseError> {
        if self.eat_if(TokenKind::Eq) || optional {
            Ok(())
        } else {
            Err(self.error(format!("expected =, got {}", self.peek_kind())))
        }
    }

    fn parse_role_options(&mut self) -> Result<HashMap<String, String>, ParseError> {
        self.expect(TokenKind::Eq)?;
        self.parse_map_literal_strings()
    }

    fn parse_role_access_option(
        &mut self,
        datacenter_access: &mut Option<RoleAccess>,
        cidr_access: &mut Option<RoleAccess>,
    ) -> Result<(), ParseError> {
        if self.eat_ident_ci("TO") {
            if self.eat_keyword(Keyword::All) {
                self.expect_ident_ci("DATACENTERS")?;
                *datacenter_access = Some(RoleAccess::All);
            } else {
                self.expect_ident_ci("DATACENTERS")?;
                *datacenter_access = Some(RoleAccess::Restricted(self.parse_string_set()?));
            }
            return Ok(());
        }

        if self.eat_keyword(Keyword::From) {
            if self.eat_keyword(Keyword::All) {
                self.expect_ident_ci("CIDRS")?;
                *cidr_access = Some(RoleAccess::All);
            } else {
                self.expect_ident_ci("CIDRS")?;
                *cidr_access = Some(RoleAccess::Restricted(self.parse_string_set()?));
            }
            return Ok(());
        }

        Err(self.error("expected ACCESS TO DATACENTERS or ACCESS FROM CIDRS".to_string()))
    }

    // ─── CREATE MATERIALIZED VIEW ───────────────────────────────────────

    fn parse_create_materialized_view(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Materialized)?;
        self.expect_keyword(Keyword::View)?;
        let if_not_exists = self.parse_if_not_exists();
        let (ks, name) = self.parse_table_name()?;
        self.expect_keyword(Keyword::As)?;
        let select = match self.parse_select()? {
            Statement::Select(s) => s,
            _ => return Err(self.error("expected SELECT after AS".into())),
        };
        self.expect_keyword(Keyword::Primary)?;
        self.expect_keyword(Keyword::Key)?;
        self.expect(TokenKind::LParen)?;
        let mut partition_key = Vec::new();
        let mut clustering_key = Vec::new();
        if self.eat_if(TokenKind::LParen) {
            partition_key = self.parse_ident_list()?;
            self.expect(TokenKind::RParen)?;
        } else {
            partition_key.push(self.expect_ident()?);
        }
        while self.eat_if(TokenKind::Comma) {
            clustering_key.push(self.expect_ident()?);
        }
        self.expect(TokenKind::RParen)?;
        let options = HashMap::new();
        let clustering_order = Vec::new();
        Ok(Statement::CreateMaterializedView(CreateMaterializedView {
            keyspace: ks,
            name,
            if_not_exists,
            select,
            partition_key,
            clustering_key,
            clustering_order,
            options,
        }))
    }

    // ─── GRANT / REVOKE ─────────────────────────────────────────────────

    fn parse_grant(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Grant)?;
        let permissions = self.parse_permission_list()?;
        self.expect_keyword(Keyword::On)?;
        let resource = self.parse_resource()?;
        // TO role
        // "TO" is not a keyword, so we match ident
        let to = self.expect_ident()?;
        if to != "to" {
            return Err(self.error(format!("expected TO, got {}", to)));
        }
        let role = self.expect_role_name(true)?;
        Ok(Statement::Grant(GrantStatement {
            permissions,
            resource,
            role,
        }))
    }

    fn parse_revoke(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Revoke)?;
        let permissions = self.parse_permission_list()?;
        self.expect_keyword(Keyword::On)?;
        let resource = self.parse_resource()?;
        self.expect_keyword(Keyword::From)?;
        let role = self.expect_role_name(true)?;
        Ok(Statement::Revoke(RevokeStatement {
            permissions,
            resource,
            role,
        }))
    }

    fn parse_permission_list(&mut self) -> Result<Vec<String>, ParseError> {
        if self.eat_keyword(Keyword::All) {
            self.eat_keyword(Keyword::Permissions);
            return Ok(vec!["ALL".to_string()]);
        }
        let mut permissions = Vec::new();
        loop {
            let perm = self.parse_permission_name()?;
            self.eat_keyword(Keyword::Permission);
            permissions.push(perm.to_uppercase());
            if !self.eat_if(TokenKind::Comma) {
                break;
            }
        }
        Ok(permissions)
    }

    fn parse_permission_name(&mut self) -> Result<String, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::Ident(name) => {
                self.advance();
                Ok(name)
            }
            TokenKind::Keyword(Keyword::Select) => {
                self.advance();
                Ok("SELECT".to_string())
            }
            TokenKind::Keyword(Keyword::Create) => {
                self.advance();
                Ok("CREATE".to_string())
            }
            TokenKind::Keyword(Keyword::Alter) => {
                self.advance();
                Ok("ALTER".to_string())
            }
            TokenKind::Keyword(Keyword::Drop) => {
                self.advance();
                Ok("DROP".to_string())
            }
            TokenKind::Keyword(Keyword::Describe) => {
                self.advance();
                Ok("DESCRIBE".to_string())
            }
            _ => Err(self.error(format!(
                "expected permission name, got {}",
                self.peek_kind()
            ))),
        }
    }

    fn parse_resource(&mut self) -> Result<Resource, ParseError> {
        if self.eat_keyword(Keyword::All) {
            if self.eat_keyword(Keyword::Keyspace) {
                // ALL KEYSPACES
                return Ok(Resource::AllKeyspaces);
            }
            if self.eat_ident_ci("KEYSPACES") {
                return Ok(Resource::AllKeyspaces);
            }
            if self.eat_keyword(Keyword::Roles) {
                return Ok(Resource::AllRoles);
            }
            if self.eat_keyword(Keyword::Function) {
                return Ok(Resource::AllFunctions);
            }
            if self.eat_ident_ci("FUNCTIONS") {
                return Ok(Resource::AllFunctions);
            }
            return Ok(Resource::AllKeyspaces);
        }
        if self.eat_keyword(Keyword::Keyspace) {
            let name = self.expect_ident()?;
            return Ok(Resource::Keyspace(name));
        }
        if self.eat_keyword(Keyword::Table) {
            let (ks, table) = self.parse_table_name()?;
            return Ok(Resource::Table {
                keyspace: ks,
                table,
            });
        }
        if self.eat_keyword(Keyword::Role) {
            let name = self.expect_role_name(true)?;
            return Ok(Resource::Role(name));
        }
        // Default: try as table reference
        let (ks, table) = self.parse_table_name()?;
        Ok(Resource::Table {
            keyspace: ks,
            table,
        })
    }

    // ─── LIST ───────────────────────────────────────────────────────────

    fn parse_list(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::List)?;
        if self.eat_keyword(Keyword::Roles) {
            let of_role = if self.eat_keyword(Keyword::Of) {
                Some(self.expect_role_name(true)?)
            } else {
                None
            };
            let no_recursive = self.eat_keyword(Keyword::Norecursive);
            Ok(Statement::ListRoles(ListRolesStatement {
                of_role,
                no_recursive,
            }))
        } else if self.eat_keyword(Keyword::Permissions) || self.eat_keyword(Keyword::Permission) {
            let permissions = vec!["ALL".to_string()];
            let resource = if self.eat_keyword(Keyword::On) {
                Some(self.parse_resource()?)
            } else {
                None
            };
            let of_role = if self.eat_keyword(Keyword::Of) {
                Some(self.expect_role_name(true)?)
            } else {
                None
            };
            Ok(Statement::ListPermissions(ListPermissionsStatement {
                permissions,
                resource,
                of_role,
            }))
        } else {
            let permissions = self.parse_permission_list()?;
            self.eat_keyword(Keyword::Permissions);
            let resource = if self.eat_keyword(Keyword::On) {
                Some(self.parse_resource()?)
            } else {
                None
            };
            let of_role = if self.eat_keyword(Keyword::Of) {
                Some(self.expect_role_name(true)?)
            } else {
                None
            };
            Ok(Statement::ListPermissions(ListPermissionsStatement {
                permissions,
                resource,
                of_role,
            }))
        }
    }

    // ─── Extra helpers ──────────────────────────────────────────────────

    fn parse_string_literal(&mut self) -> Result<String, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::StringLiteral(s) => {
                self.advance();
                Ok(s)
            }
            _ => Err(self.error(format!("expected string literal, got {}", self.peek_kind()))),
        }
    }

    fn parse_boolean(&mut self) -> Result<bool, ParseError> {
        match self.peek_kind().clone() {
            TokenKind::BooleanLiteral(b) => {
                self.advance();
                Ok(b)
            }
            _ => Err(self.error(format!("expected boolean, got {}", self.peek_kind()))),
        }
    }

    // ─── DESCRIBE ───────────────────────────────────────────────────────

    fn parse_describe(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Describe)?;

        let target = if self.eat_keyword(Keyword::Cluster) {
            DescribeTarget::Cluster
        } else if self.eat_keyword(Keyword::Full) {
            self.expect_keyword(Keyword::Schema)?;
            DescribeTarget::FullSchema
        } else if self.eat_keyword(Keyword::Schema) {
            DescribeTarget::FullSchema
        } else if self.eat_keyword(Keyword::Keyspace) {
            let name = self.expect_ident()?;
            DescribeTarget::Keyspace(name)
        } else if self.eat_keyword(Keyword::Table) {
            let (ks, name) = self.parse_table_name()?;
            DescribeTarget::Table(ks, name)
        } else if self.eat_keyword(Keyword::Type) {
            let (ks, name) = self.parse_table_name()?;
            DescribeTarget::Type(ks, name)
        } else if self.eat_keyword(Keyword::Function) {
            let (ks, name) = self.parse_table_name()?;
            DescribeTarget::Function(ks, name)
        } else if self.eat_keyword(Keyword::Aggregate) {
            let (ks, name) = self.parse_table_name()?;
            DescribeTarget::Aggregate(ks, name)
        } else {
            // Generic: DESCRIBE <name> — could be keyspace, table, etc.
            let name = self.expect_ident()?;
            DescribeTarget::Generic(name)
        };

        Ok(Statement::Describe(DescribeStatement { target }))
    }

    // ─── COMMENT ON ────────────────────────────────────────────────────

    fn parse_comment(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Comment)?;
        self.expect_keyword(Keyword::On)?;

        let target = if self.eat_keyword(Keyword::Keyspace) {
            CommentTarget::Keyspace(self.expect_ident()?)
        } else if self.eat_keyword(Keyword::Table) {
            let (keyspace, table) = self.parse_table_name()?;
            CommentTarget::Table { keyspace, table }
        } else if self.eat_keyword(Keyword::Column) {
            match self.parse_dotted_name()?.as_slice() {
                [table, column] => CommentTarget::Column {
                    keyspace: None,
                    table: table.clone(),
                    column: column.clone(),
                },
                [keyspace, table, column] => CommentTarget::Column {
                    keyspace: Some(keyspace.clone()),
                    table: table.clone(),
                    column: column.clone(),
                },
                _ => {
                    return Err(self.error(
                        "COMMENT ON COLUMN expects table.column or keyspace.table.column".into(),
                    ));
                }
            }
        } else if self.eat_keyword(Keyword::Type) {
            let (keyspace, name) = self.parse_table_name()?;
            CommentTarget::Type { keyspace, name }
        } else if self.eat_keyword(Keyword::Field) {
            match self.parse_dotted_name()?.as_slice() {
                [type_name, field] => CommentTarget::Field {
                    keyspace: None,
                    type_name: type_name.clone(),
                    field: field.clone(),
                },
                [keyspace, type_name, field] => CommentTarget::Field {
                    keyspace: Some(keyspace.clone()),
                    type_name: type_name.clone(),
                    field: field.clone(),
                },
                _ => {
                    return Err(self.error(
                        "COMMENT ON FIELD expects type.field or keyspace.type.field".into(),
                    ));
                }
            }
        } else {
            return Err(self.error(
                "expected KEYSPACE, TABLE, COLUMN, TYPE, or FIELD after COMMENT ON".into(),
            ));
        };

        self.expect_keyword(Keyword::Is)?;
        let comment = self.parse_string_literal()?;
        Ok(Statement::Comment(CommentStatement { target, comment }))
    }
}

fn is_writetime_or_ttl_selector(name: &str) -> bool {
    name.eq_ignore_ascii_case("writetime")
        || name.eq_ignore_ascii_case("maxwritetime")
        || name.eq_ignore_ascii_case("ttl")
}

fn is_term_start(kind: &TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::StringLiteral(_)
            | TokenKind::IntegerLiteral(_)
            | TokenKind::FloatLiteral(_)
            | TokenKind::BlobLiteral(_)
            | TokenKind::UuidLiteral(_)
            | TokenKind::BooleanLiteral(_)
            | TokenKind::NullLiteral
            | TokenKind::QuestionMark
            | TokenKind::NamedBind(_)
            | TokenKind::LParen
            | TokenKind::LBracket
            | TokenKind::LBrace
            | TokenKind::Ident(_)
            | TokenKind::Keyword(Keyword::Cast)
            | TokenKind::Minus
    )
}

fn is_unreserved_keyword(kw: Keyword) -> bool {
    matches!(
        kw,
        Keyword::Json
            | Keyword::Default
            | Keyword::Unset
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
            | Keyword::Role
            | Keyword::Roles
            | Keyword::Permission
            | Keyword::Permissions
            | Keyword::Function
            | Keyword::Aggregate
            | Keyword::Trigger
            | Keyword::Returns
            | Keyword::Language
            | Keyword::Called
            | Keyword::Input
            | Keyword::Sfunc
            | Keyword::Stype
            | Keyword::Finalfunc
            | Keyword::Initcond
            | Keyword::Custom
            | Keyword::Replace
            | Keyword::Login
            | Keyword::Superuser
            | Keyword::Password
            | Keyword::Norecursive
            | Keyword::Of
            | Keyword::All
            | Keyword::Cast
            | Keyword::Vector
            | Keyword::Transaction
            | Keyword::Let
            | Keyword::Returning
            | Keyword::Commit
            | Keyword::Comment
            | Keyword::Field
    )
}

fn is_column_reference(term: &Term, column: &str) -> bool {
    matches!(term, Term::Literal(Literal::String(name)) if name.eq_ignore_ascii_case(column))
}

/// Parser error.
#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Parse error at position {}: {}",
            self.position, self.message
        )
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
        let stmt =
            parse("CREATE TABLE ks.users (id uuid, name text, age int, PRIMARY KEY (id))").unwrap();
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
        let stmt =
            parse("CREATE TABLE t (a int, b int, c int, d int, PRIMARY KEY ((a, b), c))").unwrap();
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
    fn parse_select_with_like() {
        let stmt = parse("SELECT * FROM users WHERE name LIKE 'Al%' ALLOW FILTERING").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.where_clause.len(), 1);
                assert_eq!(s.where_clause[0].column, "name");
                assert_eq!(s.where_clause[0].op, RelationOp::Like);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_with_token_multi_column_and_collection_restrictions() {
        let stmt = parse("SELECT * FROM users WHERE token(id) > 0").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.where_clause[0].column, "token");
                assert_eq!(s.where_clause[0].op, RelationOp::Gt);
            }
            _ => panic!("expected Select"),
        }

        let stmt = parse("SELECT * FROM users WHERE (ck1, ck2) >= (1, 2)").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.where_clause[0].column, "(ck1,ck2)");
                assert_eq!(s.where_clause[0].op, RelationOp::Gte);
                assert!(matches!(
                    s.where_clause[0].value,
                    Term::TupleLiteral(ref values) if values.len() == 2
                ));
            }
            _ => panic!("expected Select"),
        }

        let stmt = parse("SELECT * FROM users WHERE tags CONTAINS 'x' ALLOW FILTERING").unwrap();
        match stmt {
            Statement::Select(s) => assert_eq!(s.where_clause[0].op, RelationOp::Contains),
            _ => panic!("expected Select"),
        }

        let stmt =
            parse("SELECT * FROM users WHERE attrs CONTAINS KEY 'k' ALLOW FILTERING").unwrap();
        match stmt {
            Statement::Select(s) => assert_eq!(s.where_clause[0].op, RelationOp::ContainsKey),
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
    fn parse_select_with_group_by() {
        let stmt = parse("SELECT id, count(*) FROM events GROUP BY id").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.group_by, vec!["id"]);
                assert!(matches!(s.columns, SelectColumns::Named(_)));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_writetime_ttl_and_function_aliases() {
        let stmt =
            parse("SELECT writetime(name), ttl(name) AS ttl_name, count(*), count(1) AS one_count, cast(id AS text) AS id_text FROM events")
                .unwrap();
        match stmt {
            Statement::Select(s) => {
                let SelectColumns::Named(selectors) = s.columns else {
                    panic!("expected named selectors");
                };
                assert_eq!(
                    selectors[0],
                    Selector::WritetimeOrTtl("writetime".to_string(), "name".to_string())
                );
                assert_eq!(
                    selectors[1],
                    Selector::Alias {
                        selector: Box::new(Selector::WritetimeOrTtl(
                            "ttl".to_string(),
                            "name".to_string()
                        )),
                        alias: "ttl_name".to_string(),
                    }
                );
                assert_eq!(selectors[2], Selector::Count);
                assert_eq!(
                    selectors[3],
                    Selector::Alias {
                        selector: Box::new(Selector::Count),
                        alias: "one_count".to_string(),
                    }
                );
                assert_eq!(
                    selectors[4],
                    Selector::Alias {
                        selector: Box::new(Selector::Cast {
                            selector: Box::new(Selector::Column("id".to_string())),
                            target: CqlTypeName::Simple("text".to_string()),
                        }),
                        alias: "id_text".to_string(),
                    }
                );
            }
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
    fn parse_type_hint_and_cast_terms() {
        assert_eq!(
            parse_term("(bigint) 1").unwrap(),
            Term::TypeHint(
                CqlTypeName::Simple("bigint".to_string()),
                Box::new(Term::Literal(Literal::Integer(1)))
            )
        );
        assert_eq!(
            parse_term("cast(1 AS text)").unwrap(),
            Term::TypeHint(
                CqlTypeName::Simple("text".to_string()),
                Box::new(Term::Literal(Literal::Integer(1)))
            )
        );

        let stmt = parse("INSERT INTO t (id, value) VALUES ((bigint) ?, cast(1 AS text))").unwrap();
        match stmt {
            Statement::Insert(i) => {
                assert!(matches!(i.values[0], Term::TypeHint(_, _)));
                assert!(matches!(i.values[1], Term::TypeHint(_, _)));
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
    fn parse_lwt_collection_element_conditions() {
        let stmt = parse(
            "UPDATE t SET value = 2 WHERE id = 1 IF attrs['state'] = 'open' AND scores[0] = 7",
        )
        .unwrap();
        match stmt {
            Statement::Update(update) => {
                assert_eq!(update.if_conditions.len(), 2);
                assert_eq!(update.if_conditions[0].column, "attrs");
                assert!(matches!(
                    update.if_conditions[0].value,
                    Term::CollectionElement {
                        key: ref map_key,
                        value: ref map_value
                    } if matches!(**map_key, Term::Literal(Literal::String(ref value)) if value == "state")
                        && matches!(**map_value, Term::Literal(Literal::String(ref value)) if value == "open")
                ));
                assert_eq!(update.if_conditions[1].column, "scores");
                assert!(matches!(
                    update.if_conditions[1].value,
                    Term::CollectionElement {
                        key: ref list_key,
                        value: ref list_value
                    } if matches!(**list_key, Term::Literal(Literal::Integer(0)))
                        && matches!(**list_value, Term::Literal(Literal::Integer(7)))
                ));
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn parse_set_literal_as_collection_literal() {
        let stmt = parse("UPDATE t SET tags = tags - {'old', 'stale'} WHERE id = 1").unwrap();
        match stmt {
            Statement::Update(u) => {
                assert_eq!(u.assignments[0].op, AssignmentOp::CollectionRemove);
                assert_eq!(
                    u.assignments[0].value,
                    Term::CollectionLiteral(vec![
                        Term::Literal(Literal::String("old".to_string())),
                        Term::Literal(Literal::String("stale".to_string())),
                    ])
                );
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn parse_map_literal_after_set_literal_support() {
        let stmt =
            parse("UPDATE t SET attrs = {'region': 'eu', 'tier': 'gold'} WHERE id = 1").unwrap();
        match stmt {
            Statement::Update(u) => {
                assert_eq!(u.assignments[0].op, AssignmentOp::Set);
                assert!(
                    matches!(u.assignments[0].value, Term::MapLiteral(ref entries) if entries.len() == 2)
                );
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
    fn parse_delete_allows_only_using_timestamp_like_java() {
        let stmt = parse("DELETE FROM t USING TIMESTAMP 123 WHERE id = 1").unwrap();
        match stmt {
            Statement::Delete(d) => {
                assert!(matches!(
                    d.using.as_slice(),
                    [UsingClause::Timestamp(Term::Literal(Literal::Integer(123)))]
                ));
            }
            _ => panic!("expected Delete"),
        }

        let err = parse("DELETE FROM t USING TTL 60 WHERE id = 1").unwrap_err();
        assert!(err.message.contains("expected Timestamp"));
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
        let stmt = parse("TRUNCATE ks.t").unwrap();
        match stmt {
            Statement::Truncate(t) => {
                assert_eq!(t.keyspace, Some("ks".into()));
                assert_eq!(t.table, "t");
            }
            _ => panic!("expected Truncate"),
        }

        let stmt = parse("TRUNCATE COLUMNFAMILY ks.t").unwrap();
        match stmt {
            Statement::Truncate(t) => {
                assert_eq!(t.keyspace, Some("ks".into()));
                assert_eq!(t.table, "t");
            }
            _ => panic!("expected Truncate"),
        }

        assert!(parse("TRUNCATE TABLE ks.t").is_err());
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
    fn parse_insert_json() {
        let stmt =
            parse(r#"INSERT INTO t JSON '{"id": 1, "name": "alice"}' IF NOT EXISTS"#).unwrap();
        match stmt {
            Statement::Insert(i) => {
                assert!(i.if_not_exists);
                assert!(i.columns.is_empty());
                assert!(matches!(i.json, Some(Term::Literal(Literal::String(_)))));
                assert_eq!(i.json_default, JsonDefault::Null);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_insert_json_default_unset() {
        let stmt = parse(r#"INSERT INTO t JSON '{"id": 1}' DEFAULT UNSET"#).unwrap();
        match stmt {
            Statement::Insert(i) => {
                assert!(matches!(i.json, Some(Term::Literal(Literal::String(_)))));
                assert_eq!(i.json_default, JsonDefault::Unset);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_create_table_with_options() {
        let stmt = parse(
            "CREATE TABLE t (id int PRIMARY KEY) WITH gc_grace_seconds = 86400 AND comment = 'test' AND compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 64}"
        ).unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(
                    ct.options.get("gc_grace_seconds"),
                    Some(&"86400".to_string())
                );
                assert_eq!(ct.options.get("comment"), Some(&"test".to_string()));
                assert_eq!(
                    serde_json::from_str::<std::collections::BTreeMap<String, String>>(
                        ct.options.get("compression").unwrap()
                    )
                    .unwrap()
                    .get("chunk_length_in_kb")
                    .map(String::as_str),
                    Some("64")
                );
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_create_table_with_tuple_types() {
        let stmt = parse(
            "CREATE TABLE ks.t (
                id int PRIMARY KEY,
                point tuple<double, double>,
                frozen_point frozen<tuple<int, text>>,
                attrs map<text, tuple<int, text>>
            )",
        )
        .unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                let point = ct.columns.iter().find(|col| col.name == "point").unwrap();
                assert_eq!(
                    point.cql_type,
                    CqlTypeName::Tuple(vec![
                        CqlTypeName::Simple("double".to_string()),
                        CqlTypeName::Simple("double".to_string())
                    ])
                );

                let frozen_point = ct
                    .columns
                    .iter()
                    .find(|col| col.name == "frozen_point")
                    .unwrap();
                assert_eq!(
                    frozen_point.cql_type,
                    CqlTypeName::Frozen(Box::new(CqlTypeName::Tuple(vec![
                        CqlTypeName::Simple("int".to_string()),
                        CqlTypeName::Simple("text".to_string())
                    ])))
                );

                let attrs = ct.columns.iter().find(|col| col.name == "attrs").unwrap();
                assert_eq!(
                    attrs.cql_type,
                    CqlTypeName::Map(
                        Box::new(CqlTypeName::Simple("text".to_string())),
                        Box::new(CqlTypeName::Tuple(vec![
                            CqlTypeName::Simple("int".to_string()),
                            CqlTypeName::Simple("text".to_string())
                        ]))
                    )
                );
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_create_table_with_vector_type() {
        let stmt = parse("CREATE TABLE ks.items (id int PRIMARY KEY, embedding vector<float, 3>)")
            .unwrap();
        match stmt {
            Statement::CreateTable(ct) => {
                let embedding = ct
                    .columns
                    .iter()
                    .find(|col| col.name == "embedding")
                    .unwrap();
                assert_eq!(
                    embedding.cql_type,
                    CqlTypeName::Vector(Box::new(CqlTypeName::Simple("float".to_string())), 3)
                );
            }
            _ => panic!("expected CreateTable"),
        }
    }

    #[test]
    fn parse_vector_type_rejects_bad_dimension() {
        assert!(
            parse("CREATE TABLE ks.items (id int PRIMARY KEY, embedding vector<float, 0>)")
                .is_err()
        );
    }

    #[test]
    fn parse_select_with_order_by() {
        let stmt = parse("SELECT * FROM t WHERE pk = 1 ORDER BY ck DESC").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.order_by.len(), 1);
                assert_eq!(s.order_by[0].1, ClusteringOrder::Desc);
                assert!(s.ann_order_by.is_none());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn parse_select_with_ann_order_by() {
        let stmt =
            parse("SELECT * FROM ks.items ORDER BY embedding ANN OF [1.0, -2, 3] LIMIT 5").unwrap();
        match stmt {
            Statement::Select(s) => {
                assert!(s.order_by.is_empty());
                assert_eq!(s.limit, Some(Term::Literal(Literal::Integer(5))));
                let ann = s.ann_order_by.unwrap();
                assert_eq!(ann.column, "embedding");
                assert_eq!(ann.vector_literal, vec![1.0, -2.0, 3.0]);
            }
            _ => panic!("expected Select"),
        }
    }

    // ── Phase 12 tests ──

    #[test]
    fn parse_create_index() {
        let stmt = parse("CREATE INDEX idx_name ON ks.t (col)").unwrap();
        match stmt {
            Statement::CreateIndex(ci) => {
                assert_eq!(ci.name, Some("idx_name".into()));
                assert_eq!(ci.table, "t");
                assert_eq!(ci.column, "col");
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn parse_create_custom_index_with_options() {
        let stmt = parse(
            "CREATE CUSTOM INDEX embedding_idx ON ks.items (embedding) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' WITH OPTIONS = {'vector_similarity_metric': 'dot_product'}",
        )
        .unwrap();
        match stmt {
            Statement::CreateIndex(ci) => {
                assert_eq!(
                    ci.custom_class.as_deref(),
                    Some("org.apache.cassandra.index.sai.StorageAttachedIndex")
                );
                assert_eq!(
                    ci.options
                        .get("vector_similarity_metric")
                        .map(String::as_str),
                    Some("dot_product")
                );
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn parse_drop_index() {
        let stmt = parse("DROP INDEX IF EXISTS ks.my_idx").unwrap();
        match stmt {
            Statement::DropIndex(di) => {
                assert!(di.if_exists);
                assert_eq!(di.name, "my_idx");
            }
            _ => panic!("expected DropIndex"),
        }
    }

    #[test]
    fn parse_create_type() {
        let stmt = parse("CREATE TYPE ks.address (street text, city text, zip int)").unwrap();
        match stmt {
            Statement::CreateType(ct) => {
                assert_eq!(ct.name, "address");
                assert_eq!(ct.fields.len(), 3);
            }
            _ => panic!("expected CreateType"),
        }
    }

    #[test]
    fn parse_alter_type_operations() {
        let stmt = parse("ALTER TYPE ks.address ADD country text").unwrap();
        match stmt {
            Statement::AlterType(at) => {
                assert_eq!(at.keyspace.as_deref(), Some("ks"));
                assert_eq!(at.name, "address");
                assert!(matches!(
                    at.operation,
                    AlterTypeOp::AddField(ref name, CqlTypeName::Simple(ref typ))
                        if name == "country" && typ == "text"
                ));
            }
            _ => panic!("expected AlterType ADD"),
        }

        let stmt = parse("ALTER TYPE ks.address RENAME zip TO postal_code").unwrap();
        match stmt {
            Statement::AlterType(at) => {
                assert!(matches!(
                    at.operation,
                    AlterTypeOp::RenameField(ref from, ref to)
                        if from == "zip" && to == "postal_code"
                ));
            }
            _ => panic!("expected AlterType RENAME"),
        }

        let stmt = parse("ALTER TYPE ks.address ALTER postal_code TYPE bigint").unwrap();
        match stmt {
            Statement::AlterType(at) => {
                assert!(matches!(
                    at.operation,
                    AlterTypeOp::AlterFieldType(ref name, CqlTypeName::Simple(ref typ))
                        if name == "postal_code" && typ == "bigint"
                ));
            }
            _ => panic!("expected AlterType ALTER"),
        }
    }

    #[test]
    fn parse_create_role() {
        let stmt = parse(
            "CREATE ROLE admin WITH PASSWORD = 'secret' AND SUPERUSER = true AND LOGIN = true AND ACCESS TO DATACENTERS {'dc1', 'dc2'} AND ACCESS FROM ALL CIDRS",
        )
        .unwrap();
        match stmt {
            Statement::CreateRole(cr) => {
                assert_eq!(cr.name, "admin");
                assert_eq!(cr.password, Some("secret".into()));
                assert_eq!(cr.superuser, Some(true));
                assert_eq!(cr.login, Some(true));
                assert_eq!(
                    cr.datacenter_access,
                    Some(RoleAccess::Restricted(vec!["dc1".into(), "dc2".into()]))
                );
                assert_eq!(cr.cidr_access, Some(RoleAccess::All));
            }
            _ => panic!("expected CreateRole"),
        }
    }

    #[test]
    fn parse_role_hashed_password_options() {
        let stmt = parse("CREATE ROLE admin WITH HASHED PASSWORD = '$2b$12$hash' AND LOGIN = true")
            .unwrap();
        match stmt {
            Statement::CreateRole(cr) => {
                assert_eq!(cr.name, "admin");
                assert_eq!(cr.password, None);
                assert_eq!(cr.hashed_password, Some("$2b$12$hash".into()));
                assert_eq!(cr.login, Some(true));
            }
            _ => panic!("expected CreateRole"),
        }

        let stmt = parse("ALTER ROLE admin WITH HASHED PASSWORD = '$2b$12$newhash'").unwrap();
        match stmt {
            Statement::AlterRole(ar) => {
                assert_eq!(ar.name, "admin");
                assert_eq!(ar.password, None);
                assert_eq!(ar.hashed_password, Some("$2b$12$newhash".into()));
            }
            _ => panic!("expected AlterRole"),
        }
    }

    #[test]
    fn parse_role_custom_options() {
        let stmt = parse("CREATE ROLE r WITH OPTIONS = {'a':'b', 'b':1}").unwrap();
        match stmt {
            Statement::CreateRole(cr) => {
                assert_eq!(cr.name, "r");
                assert_eq!(cr.options.get("a").map(String::as_str), Some("b"));
                assert_eq!(cr.options.get("b").map(String::as_str), Some("1"));
            }
            _ => panic!("expected CreateRole"),
        }

        let stmt = parse("ALTER ROLE r WITH LOGIN = true AND OPTIONS = {'region':'west'}").unwrap();
        match stmt {
            Statement::AlterRole(ar) => {
                assert_eq!(ar.name, "r");
                assert_eq!(ar.login, Some(true));
                assert_eq!(ar.options.get("region").map(String::as_str), Some("west"));
            }
            _ => panic!("expected AlterRole"),
        }

        assert!(parse("CREATE ROLE r WITH OPTIONS = 'term'").is_err());
        assert!(parse("ALTER ROLE r WITH OPTIONS = 99").is_err());
    }

    #[test]
    fn parse_user_aliases_to_role_statements() {
        let stmt = parse("CREATE USER IF NOT EXISTS alice WITH HASHED PASSWORD '$2b$12$userhash'")
            .unwrap();
        match stmt {
            Statement::CreateRole(cr) => {
                assert_eq!(cr.name, "alice");
                assert!(cr.if_not_exists);
                assert_eq!(cr.login, Some(true));
                assert_eq!(cr.hashed_password.as_deref(), Some("$2b$12$userhash"));
            }
            _ => panic!("expected CreateRole"),
        }

        let stmt = parse("ALTER USER IF EXISTS alice WITH PASSWORD 'secret'").unwrap();
        match stmt {
            Statement::AlterRole(ar) => {
                assert_eq!(ar.name, "alice");
                assert!(ar.if_exists);
                assert_eq!(ar.password.as_deref(), Some("secret"));
            }
            _ => panic!("expected AlterRole"),
        }

        let stmt = parse("DROP USER IF EXISTS alice").unwrap();
        match stmt {
            Statement::DropRole(dr) => {
                assert_eq!(dr.name, "alice");
                assert!(dr.if_exists);
            }
            _ => panic!("expected DropRole"),
        }
    }

    #[test]
    fn parse_role_and_user_literal_name_forms() {
        let stmt = parse("CREATE ROLE 'r1'").unwrap();
        match stmt {
            Statement::CreateRole(cr) => assert_eq!(cr.name, "r1"),
            _ => panic!("expected CreateRole"),
        }

        let stmt = parse(r#"ALTER ROLE "RoleName" WITH PASSWORD = 'secret'"#).unwrap();
        match stmt {
            Statement::AlterRole(ar) => assert_eq!(ar.name, "RoleName"),
            _ => panic!("expected AlterRole"),
        }

        let stmt = parse("DROP ROLE $$ r1 ' x $ x ' $$").unwrap();
        match stmt {
            Statement::DropRole(dr) => assert_eq!(dr.name, " r1 ' x $ x ' "),
            _ => panic!("expected DropRole"),
        }

        let stmt = parse("GRANT ALTER ON ROLE 'source' TO $$ target $$").unwrap();
        match stmt {
            Statement::Grant(grant) => {
                assert_eq!(grant.role, " target ");
                assert_eq!(grant.resource, Resource::Role("source".to_string()));
            }
            _ => panic!("expected Grant"),
        }

        assert!(parse(r#"CREATE USER "u1""#).is_err());
        assert!(parse(r#"ALTER USER "u1" WITH PASSWORD 'secret'"#).is_err());
        assert!(parse(r#"DROP USER "u1""#).is_err());
    }

    #[test]
    fn parse_alter_role_access_restrictions() {
        let stmt = parse("ALTER ROLE analyst WITH ACCESS TO ALL DATACENTERS AND ACCESS FROM CIDRS {'region1', 'region2'}").unwrap();
        match stmt {
            Statement::AlterRole(ar) => {
                assert_eq!(ar.name, "analyst");
                assert_eq!(ar.datacenter_access, Some(RoleAccess::All));
                assert_eq!(
                    ar.cidr_access,
                    Some(RoleAccess::Restricted(vec![
                        "region1".into(),
                        "region2".into()
                    ]))
                );
            }
            _ => panic!("expected AlterRole"),
        }
    }

    #[test]
    fn parse_drop_role() {
        let stmt = parse("DROP ROLE IF EXISTS test_role").unwrap();
        match stmt {
            Statement::DropRole(dr) => {
                assert!(dr.if_exists);
                assert_eq!(dr.name, "test_role");
            }
            _ => panic!("expected DropRole"),
        }
    }

    #[test]
    fn parse_grant_and_list_permissions() {
        let stmt = parse("GRANT SELECT ON TABLE ks.events TO analyst").unwrap();
        match stmt {
            Statement::Grant(grant) => {
                assert_eq!(grant.permissions, vec!["SELECT"]);
                assert_eq!(grant.role, "analyst");
                assert!(matches!(
                    grant.resource,
                    Resource::Table {
                        keyspace: Some(ref keyspace),
                        ref table
                    } if keyspace == "ks" && table == "events"
                ));
            }
            _ => panic!("expected Grant"),
        }

        let stmt =
            parse("GRANT MODIFY PERMISSION, SELECT PERMISSION ON ALL KEYSPACES TO 'analyst'")
                .unwrap();
        match stmt {
            Statement::Grant(grant) => {
                assert_eq!(grant.permissions, vec!["MODIFY", "SELECT"]);
                assert_eq!(grant.role, "analyst");
                assert_eq!(grant.resource, Resource::AllKeyspaces);
            }
            _ => panic!("expected Grant"),
        }

        let stmt = parse("REVOKE CREATE, ALTER ON ROLE $$source$$ FROM $$ target $$").unwrap();
        match stmt {
            Statement::Revoke(revoke) => {
                assert_eq!(revoke.permissions, vec!["CREATE", "ALTER"]);
                assert_eq!(revoke.role, " target ");
                assert_eq!(revoke.resource, Resource::Role("source".to_string()));
            }
            _ => panic!("expected Revoke"),
        }

        let stmt = parse("LIST SELECT PERMISSION ON TABLE ks.events OF analyst").unwrap();
        match stmt {
            Statement::ListPermissions(list) => {
                assert_eq!(list.permissions, vec!["SELECT"]);
                assert_eq!(list.of_role.as_deref(), Some("analyst"));
                assert!(matches!(
                    list.resource,
                    Some(Resource::Table {
                        keyspace: Some(ref keyspace),
                        ref table
                    }) if keyspace == "ks" && table == "events"
                ));
            }
            _ => panic!("expected ListPermissions"),
        }

        let stmt = parse("LIST ALL PERMISSIONS ON ALL ROLES OF $$ r '1' $$").unwrap();
        match stmt {
            Statement::ListPermissions(list) => {
                assert_eq!(list.permissions, vec!["ALL"]);
                assert_eq!(list.resource, Some(Resource::AllRoles));
                assert_eq!(list.of_role.as_deref(), Some(" r '1' "));
            }
            _ => panic!("expected ListPermissions"),
        }

        let stmt = parse("LIST ALTER, DROP PERMISSION ON ROLE 'source' OF \"target\"").unwrap();
        match stmt {
            Statement::ListPermissions(list) => {
                assert_eq!(list.permissions, vec!["ALTER", "DROP"]);
                assert_eq!(list.resource, Some(Resource::Role("source".to_string())));
                assert_eq!(list.of_role.as_deref(), Some("target"));
            }
            _ => panic!("expected ListPermissions"),
        }
    }

    #[test]
    fn parse_create_trigger() {
        let stmt =
            parse("CREATE TRIGGER my_trigger ON ks.t USING 'org.example.MyTrigger'").unwrap();
        match stmt {
            Statement::CreateTrigger(ct) => {
                assert_eq!(ct.name, "my_trigger");
                assert_eq!(ct.table, "t");
                assert_eq!(ct.trigger_class, "org.example.MyTrigger");
            }
            _ => panic!("expected CreateTrigger"),
        }
    }

    #[test]
    fn parse_drop_trigger() {
        let stmt = parse("DROP TRIGGER my_trigger ON ks.t").unwrap();
        match stmt {
            Statement::DropTrigger(dt) => {
                assert_eq!(dt.name, "my_trigger");
                assert_eq!(dt.table, "t");
            }
            _ => panic!("expected DropTrigger"),
        }
    }

    #[test]
    fn parse_drop_type() {
        let stmt = parse("DROP TYPE IF EXISTS ks.address").unwrap();
        assert!(matches!(stmt, Statement::DropType(_)));
    }

    #[test]
    fn parse_drop_materialized_view() {
        let stmt = parse("DROP MATERIALIZED VIEW IF EXISTS ks.my_view").unwrap();
        assert!(matches!(stmt, Statement::DropMaterializedView(_)));
    }

    #[test]
    fn parse_comment_on_schema_elements() {
        let stmt = parse("COMMENT ON KEYSPACE ks IS 'primary keyspace'").unwrap();
        match stmt {
            Statement::Comment(comment) => {
                assert_eq!(comment.target, CommentTarget::Keyspace("ks".to_string()));
                assert_eq!(comment.comment, "primary keyspace");
            }
            _ => panic!("expected Comment"),
        }

        let stmt = parse("COMMENT ON TABLE ks.events IS 'event table'").unwrap();
        match stmt {
            Statement::Comment(comment) => {
                assert_eq!(
                    comment.target,
                    CommentTarget::Table {
                        keyspace: Some("ks".to_string()),
                        table: "events".to_string()
                    }
                );
                assert_eq!(comment.comment, "event table");
            }
            _ => panic!("expected Comment"),
        }

        let stmt = parse("COMMENT ON COLUMN ks.events.name IS 'display name'").unwrap();
        match stmt {
            Statement::Comment(comment) => {
                assert_eq!(
                    comment.target,
                    CommentTarget::Column {
                        keyspace: Some("ks".to_string()),
                        table: "events".to_string(),
                        column: "name".to_string()
                    }
                );
                assert_eq!(comment.comment, "display name");
            }
            _ => panic!("expected Comment"),
        }
    }

    #[test]
    fn parse_comment_on_user_type_and_field() {
        let stmt = parse("COMMENT ON TYPE ks.address IS 'postal address'").unwrap();
        match stmt {
            Statement::Comment(comment) => {
                assert_eq!(
                    comment.target,
                    CommentTarget::Type {
                        keyspace: Some("ks".to_string()),
                        name: "address".to_string()
                    }
                );
                assert_eq!(comment.comment, "postal address");
            }
            _ => panic!("expected Comment"),
        }

        let stmt = parse("COMMENT ON FIELD ks.address.street IS 'street line'").unwrap();
        match stmt {
            Statement::Comment(comment) => {
                assert_eq!(
                    comment.target,
                    CommentTarget::Field {
                        keyspace: Some("ks".to_string()),
                        type_name: "address".to_string(),
                        field: "street".to_string()
                    }
                );
                assert_eq!(comment.comment, "street line");
            }
            _ => panic!("expected Comment"),
        }
    }

    #[test]
    fn parse_transaction_statement() {
        let cql = r#"
            BEGIN TRANSACTION
                LET row1 = (SELECT * FROM ks.t WHERE id = 1);
                INSERT INTO ks.t (id, v) VALUES (2, 'hello');
            COMMIT TRANSACTION
        "#;
        let stmt = parse(cql).unwrap();
        match stmt {
            Statement::Transaction(t) => {
                assert_eq!(t.let_bindings.len(), 1);
                assert_eq!(t.let_bindings[0].name, "row1");
                assert_eq!(t.statements.len(), 1);
                assert!(t.returning.is_none());
            }
            _ => panic!("Expected Transaction"),
        }
    }

    #[test]
    fn parse_transaction_with_returning() {
        let cql = r#"
            BEGIN TRANSACTION
                LET r = (SELECT v FROM ks.t WHERE id = 1);
                UPDATE ks.t SET v = 10 WHERE id = 1;
            COMMIT TRANSACTION
            RETURNING v
        "#;
        let stmt = parse(cql).unwrap();
        match stmt {
            Statement::Transaction(t) => {
                assert_eq!(t.let_bindings.len(), 1);
                assert_eq!(t.statements.len(), 1);
                assert!(t.returning.is_some());
                let ret = t.returning.unwrap();
                assert_eq!(ret.columns.len(), 1);
            }
            _ => panic!("Expected Transaction"),
        }
    }

    #[test]
    fn parse_transaction_no_let() {
        let cql = r#"
            BEGIN TRANSACTION
                INSERT INTO ks.t (id, v) VALUES (1, 'a');
                DELETE FROM ks.t WHERE id = 2;
            COMMIT TRANSACTION
        "#;
        let stmt = parse(cql).unwrap();
        match stmt {
            Statement::Transaction(t) => {
                assert!(t.let_bindings.is_empty());
                assert_eq!(t.statements.len(), 2);
                assert!(t.returning.is_none());
            }
            _ => panic!("Expected Transaction"),
        }
    }

    #[test]
    fn parse_batch_still_works() {
        let cql = "BEGIN BATCH INSERT INTO t (id) VALUES (1); APPLY BATCH";
        let stmt = parse(cql).unwrap();
        assert!(matches!(stmt, Statement::Batch(_)));
    }

    #[test]
    fn parse_batch_rejects_non_mutation_statement_like_java() {
        let cql = "BEGIN BATCH SELECT * FROM t; APPLY BATCH";
        let err = parse(cql).unwrap_err();
        assert!(
            err.message
                .contains("expected INSERT, UPDATE, DELETE, or APPLY in BATCH")
        );

        let cql = "BEGIN BATCH USE ks; APPLY BATCH";
        let err = parse(cql).unwrap_err();
        assert!(
            err.message
                .contains("expected INSERT, UPDATE, DELETE, or APPLY in BATCH")
        );
    }
}
