use std::{fmt::Display, iter::Peekable};

use super::{
    constant::Value,
    expression::Expression,
    lexer::{Lexer, LexerError, LexerResult},
    predicate::Predicate,
    term::Term,
    token::Token,
};

pub type FieldName = String;
pub type IndexName = String;
pub type TableName = String;
pub type ViewName = String;

#[derive(Debug, PartialEq, Eq)]
pub struct DeleteNode(pub TableName, pub Option<Predicate>);

#[derive(Debug, PartialEq, Eq)]
pub struct InsertNode(pub TableName, pub Vec<FieldName>, pub Vec<Value>);

#[derive(Debug, PartialEq, Eq)]
pub struct SelectNode {
    pub fields: Vec<FieldName>,
    pub tables: Vec<TableName>,
    pub predicate: Option<Predicate>,
}

impl Display for SelectNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let field_names = self.fields.join(", ");
        let table_names = self.tables.join(", ");
        let pred = if let Some(pred) = &self.predicate {
            format!(" WHERE {}", pred)
        } else {
            "".to_string()
        };

        write!(f, "SELECT {} FROM {}{}", field_names, table_names, pred)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct UpdateNode {
    pub id: String,
    pub field: FieldName,
    pub expr: Expression,
    pub where_clause: Option<Predicate>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FieldType {
    Int,
    Varchar(i32),
}

#[derive(Debug, PartialEq, Eq)]
pub struct FieldDefinition(pub FieldName, pub FieldType);
pub type FieldDefinitions = Vec<FieldDefinition>;

#[derive(Debug, PartialEq, Eq)]
pub enum CreateNode {
    Table(TableName, FieldDefinitions),
    View(ViewName, SelectNode),
    Index(IndexName, TableName, FieldName),
}

#[derive(Debug, PartialEq, Eq)]
pub enum RootNode {
    Select(SelectNode),
    Insert(InsertNode),
    Delete(DeleteNode),
    Update(UpdateNode),
    Create(CreateNode),
}

pub struct Parser<'a> {
    //lexer: Lexer<'a>,
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(lexer: Lexer<'a>) -> Self {
        Self {
            lexer: lexer.peekable(),
        }
    }

    fn parse_varchar_type(&mut self) -> Result<FieldType, String> {
        self.expect_token(Token::LeftParen)?;

        if let Token::IntegerConst(int) = self.next_token()? {
            self.expect_token(Token::RightParen)?;

            Ok(FieldType::Varchar(int))
        } else {
            Err("Did not find expected varchar length".to_string())
        }
    }

    fn parse_constant(&mut self) -> Result<Value, String> {
        let next_token = self.next_token()?;
        match next_token {
            Token::VarcharConst(val) => Ok(Value::Varchar(val)),
            Token::IntegerConst(val) => Ok(Value::Int(val)),
            _ => Err(format!("Expected constant, found {:?}", next_token)),
        }
    }

    fn parse_expression(&mut self) -> Result<Expression, String> {
        let next_token = self.next_token()?;
        match next_token {
            Token::Identifier(id) => Ok(Expression::Field(id)),
            Token::VarcharConst(val) => Ok(Expression::Constant(Value::Varchar(val))),
            Token::IntegerConst(val) => Ok(Expression::Constant(Value::Int(val))),
            _ => Err(format!(
                "Invalid token found in expression: {:?}",
                next_token
            )),
        }
    }

    fn parse_term(&mut self) -> Result<Term, String> {
        let lexpr = self.parse_expression()?;
        self.expect_token(Token::Equal)?;
        let rexpr = self.parse_expression()?;

        Ok(Term::new(lexpr, rexpr))
    }

    fn parse_predicate(&mut self) -> Result<Predicate, String> {
        let mut terms = vec![self.parse_term()?];

        while self.next_token_is(Token::And) {
            self.expect_token(Token::And)?;
            terms.push(self.parse_term()?);
        }

        Ok(Predicate::from_terms(terms))
    }

    fn parse_type_def(&mut self) -> Result<FieldType, String> {
        match self.next_token()? {
            Token::Int => Ok(FieldType::Int),
            Token::Varchar => Ok(self.parse_varchar_type()?),
            _ => Err("expected type definition".to_string()),
        }
    }

    fn parse_identifier(&mut self) -> Result<String, String> {
        let tok = self.next_token()?;
        if let Token::Identifier(id) = tok {
            Ok(id)
        } else {
            Err(format!("expected identifier token, found {:?}", tok))
        }
    }

    fn parse_field_def(&mut self) -> Result<FieldDefinition, String> {
        Ok(FieldDefinition(
            self.parse_identifier()?,
            self.parse_type_def()?,
        ))
    }

    fn parse_field_defs(&mut self) -> Result<FieldDefinitions, String> {
        match self.next_token()? {
            Token::LeftParen => {
                let mut field_defs: FieldDefinitions = vec![self.parse_field_def()?];

                loop {
                    match self.next_token()? {
                        Token::Comma => field_defs.push(self.parse_field_def()?),
                        Token::RightParen => return Ok(field_defs),
                        _ => {
                            return Err("did not finding closing parenthesis of field definitions"
                                .to_string())
                        }
                    }
                }
            }
            _ => Err("failed to read left parenthesis of field definitions".to_string()),
        }
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>, String> {
        let mut identifiers = Vec::new();

        loop {
            identifiers.push(self.parse_identifier()?);
            if !self.next_token_is(Token::Comma) {
                break;
            }
            // eat the comma
            self.expect_token(Token::Comma)?;
        }

        Ok(identifiers)
    }

    fn parse_field_list(&mut self) -> Result<Vec<FieldName>, String> {
        self.expect_token(Token::LeftParen)?;

        let fields = self.parse_identifier_list()?;

        self.expect_token(Token::RightParen)?;
        Ok(fields)
    }

    fn parse_constant_list(&mut self) -> Result<Vec<Value>, String> {
        self.expect_token(Token::LeftParen)?;

        let mut constants = Vec::new();

        loop {
            constants.push(self.parse_constant()?);

            if !self.next_token_is(Token::Comma) {
                break;
            }
            // eat the comma
            self.expect_token(Token::Comma)?;
        }

        self.expect_token(Token::RightParen)?;
        Ok(constants)
    }

    fn parse_create_table(&mut self) -> Result<CreateNode, String> {
        if let Token::Identifier(table_name) = self.next_token()? {
            Ok(CreateNode::Table(table_name, self.parse_field_defs()?))
        } else {
            Err("failed to parse CREATE TABLE".to_string())
        }
    }

    fn parse_create_view(&mut self) -> Result<CreateNode, String> {
        if let Token::Identifier(view_name) = self.next_token()?
            && self.expect_token(Token::As)?
            && self.expect_token(Token::Select)?
        {
            Ok(CreateNode::View(view_name, self.parse_select()?))
        } else {
            Err("failed to parse CREATE VIEW statement".to_string())
        }
    }

    fn parse_create_index(&mut self) -> Result<CreateNode, String> {
        if let Token::Identifier(index_name) = self.next_token()?
            && self.expect_token(Token::On)?
            && let Token::Identifier(table_name) = self.next_token()?
            && self.expect_token(Token::LeftParen)?
            && let Token::Identifier(field_name) = self.next_token()?
            && self.expect_token(Token::RightParen)?
        {
            Ok(CreateNode::Index(index_name, table_name, field_name))
        } else {
            Err("failed to parse CREATE INDEX statement".to_string())
        }
    }

    fn parse_create(&mut self) -> Result<CreateNode, String> {
        match self.next_token()? {
            Token::Index => self.parse_create_index(),
            Token::Table => self.parse_create_table(),
            Token::View => self.parse_create_view(),
            _ => Err("Did not find expected INDEX, TABLE, or VIEW identifier".to_string()),
        }
    }

    fn expect_token(&mut self, tok: Token) -> Result<bool, String> {
        let next_tok = self.next_token();
        match next_tok {
            Ok(tok) => Ok(true),
            _ => Err(format!(
                "expected token: {:?} but found {:?}",
                tok, next_tok
            )),
        }
    }

    fn next_token(&mut self) -> Result<Token, String> {
        self.lexer
            .next()
            .ok_or_else(|| "reached unexpected end of input".to_string())
            .and_then(|res| res.map_err((|e| format!("lexer error: {:?}", e))))
    }

    fn next_token_is(&mut self, tok: Token) -> bool {
        match self.lexer.peek() {
            None => false,
            Some(res) => match res {
                Ok(t) if *t == tok => true,
                _ => false,
            },
        }
    }

    fn parse_where_clause(&mut self) -> Result<Predicate, String> {
        self.parse_predicate()
    }

    fn parse_optional_where_clause(&mut self) -> Result<Option<Predicate>, String> {
        if self.next_token_is(Token::Where) {
            // eat the `WHERE` token
            assert!(self.expect_token(Token::Where)?);

            Ok(Some(self.parse_where_clause()?))
        } else {
            Ok(None)
        }
    }

    fn parse_update(&mut self) -> Result<UpdateNode, String> {
        let next = self.next_token()?;
        if let Token::Identifier(id_name) = next {
            self.expect_token(Token::Set)?;
            let field_name = self.parse_identifier()?;
            self.expect_token(Token::Equal)?;
            let expr = self.parse_expression()?;

            let where_clause = self.parse_optional_where_clause()?;

            Ok(UpdateNode {
                id: id_name,
                field: field_name,
                expr,
                where_clause,
            })
        } else {
            Err("failed to parse id name in update clause".to_string())
        }
    }

    fn parse_delete(&mut self) -> Result<DeleteNode, String> {
        if Token::From == self.next_token()?
            && let Token::Identifier(table_name) = self.next_token()?
        {
            let where_clause = self.parse_optional_where_clause()?;
            Ok(DeleteNode(table_name, where_clause))
        } else {
            Err("Failed to parse delete clause".to_string())
        }
    }

    fn parse_insert(&mut self) -> Result<InsertNode, String> {
        self.expect_token(Token::Into)?;

        if let Token::Identifier(table_name) = self.next_token()? {
            let field_list = self.parse_field_list()?;
            self.expect_token(Token::Values)?;
            let const_list = self.parse_constant_list()?;

            Ok(InsertNode(table_name, field_list, const_list))
        } else {
            Err("expected table identifier".to_string())
        }
    }

    fn parse_table_list(&mut self) -> Result<Vec<TableName>, String> {
        self.parse_identifier_list()
    }

    fn parse_select_list(&mut self) -> Result<Vec<FieldName>, String> {
        self.parse_identifier_list()
    }

    fn parse_select(&mut self) -> Result<SelectNode, String> {
        let select_list = self.parse_select_list()?;
        self.expect_token(Token::From)?;
        let table_list = self.parse_table_list()?;
        let where_clause = self.parse_optional_where_clause()?;

        Ok(SelectNode {
            fields: select_list,
            tables: table_list,
            predicate: where_clause,
        })
    }

    pub fn parse(&mut self) -> Result<RootNode, String> {
        self.lexer
            .next()
            .ok_or_else(|| "No input provided".to_string())
            .and_then(|tok| match tok {
                Ok(Token::Create) => self.parse_create().map(RootNode::Create),
                Ok(Token::Update) => self.parse_update().map(RootNode::Update),
                Ok(Token::Delete) => self.parse_delete().map(RootNode::Delete),
                Ok(Token::Insert) => self.parse_insert().map(RootNode::Insert),
                Ok(Token::Select) => self.parse_select().map(RootNode::Select),
                Ok(_) | Err(_) => Err("Failed to parse root statement".to_string()),
            })
    }
}

pub fn parse(text: &str) -> Result<RootNode, String> {
    Parser::new(Lexer::new(text)).parse()
}

#[cfg(test)]
mod tests {
    use crate::parser::{lexer::Lexer, parser::*};

    use super::Parser;

    macro_rules! parser_tests {
        ($($name:ident: $input:expr => $expected:expr,)*) => {
            $(
                #[test]
                fn $name() {
                    let mut lex = Lexer::new($input);
                    let mut parser = Parser::new(lex);

                    let ast = parser.parse();
                    assert_eq!(ast, $expected);
                }
            )*
        }
    }

    parser_tests! {
        test_parser_create_table_1: "CREATE TABLE test ( id int, name varchar(10))" =>
            Ok(
                RootNode::Create(
                    CreateNode::Table("test".to_string(), vec![
                        FieldDefinition("id".to_string(), FieldType::Int),
                        FieldDefinition("name".to_string(), FieldType::Varchar(10))]))),

        test_parser_create_index_1: "CREATE INDEX idx_test ON test_table ( test_field )" =>
            Ok(
                RootNode::Create(
                    CreateNode::Index("idx_test".to_string(), "test_table".to_string(), "test_field".to_string())
                )
            ),

        test_parser_create_view_1: "CREATE VIEW view_test AS SELECT f1, f2 FROM test_table" =>
            Ok(
                RootNode::Create(
                    CreateNode::View(
                        "view_test".to_string(),
                        SelectNode{
                            fields: vec!["f1".to_string(), "f2".to_string()],
                            tables: vec!["test_table".to_string()],
                            predicate: None,
                        }
                    )
                )
            ),


        test_parser_update_1: "UPDATE test_table SET test_field = 10" =>
            Ok(
                RootNode::Update(
                    UpdateNode{
                        id: "test_table".to_string(),
                        field: "test_field".to_string(),
                        expr: Expression::Constant(Value::Int(10)),
                        where_clause: None})
            ),

        test_parser_update_2: "UPDATE test_table SET test_field = 10 WHERE other_field = 'testing!'" =>
            Ok(
                RootNode::Update(
                    UpdateNode{
                        id: "test_table".to_string(),
                        field: "test_field".to_string(),
                        expr: Expression::Constant(Value::Int(10)),
                        where_clause: Some(Predicate::from_term(
                                Term::new(
                                    Expression::Field("other_field".to_string()),
                                    Expression::Constant(Value::Varchar("testing!".to_string()))
                                )
                        ))
                    })
            ),

        test_parser_delete_1: "DELETE FROM test_table" =>
            Ok(
                RootNode::Delete(
                    DeleteNode("test_table".to_string(), None)
                )
            ),

        test_parser_delete_2: "DELETE FROM test_table WHERE field = 'testing!'" =>
            Ok(
                RootNode::Delete(
                    DeleteNode("test_table".to_string(),
                        Some(Predicate::from_term(
                            Term::new(
                                Expression::Field("field".to_string()),
                                Expression::Constant(Value::Varchar("testing!".to_string()))
                            )))
                    )
                )
            ),

        test_parser_insert: "INSERT INTO test_table ( a, b, c) VALUES (1, 'test1', 'test2')" =>
            Ok(
                RootNode::Insert(
                    InsertNode("test_table".to_string(),
                        vec!["a".to_string(), "b".to_string(), "c".to_string()],
                        vec![
                            Value::Int(1),
                            Value::Varchar("test1".to_string()),
                            Value::Varchar("test2".to_string())])
                )
            ),

        test_parser_select: "SELECT a, b, c FROM t1, t2 WHERE a = c" =>
            Ok(
                RootNode::Select(
                    SelectNode{
                        fields: vec!["a".to_string(), "b".to_string(), "c".to_string()],
                        tables: vec!["t1".to_string(), "t2".to_string()],
                        predicate: Some(Predicate::from_term(
                            Term::new(
                                Expression::Field("a".to_string()),
                                Expression::Field("c".to_string())
                            )))}
                )
            ),
    }
}
