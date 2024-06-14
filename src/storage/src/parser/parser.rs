use super::{lexer::Lexer, token::Token};

pub type FieldName = String;
pub type IndexName = String;
pub type TableName = String;
pub type ViewName = String;

#[derive(Debug, PartialEq, Eq)]
pub enum DeleteNode {}

#[derive(Debug, PartialEq, Eq)]
pub enum InsertNode {}

#[derive(Debug, PartialEq, Eq)]
pub enum SelectNode {}

#[derive(Debug, PartialEq, Eq)]
pub enum UpdateNode {}

#[derive(Debug, PartialEq, Eq)]
pub enum FieldType {
    Int,
    Varchar(i32),
}

#[derive(Debug, PartialEq, Eq)]
pub struct FieldDefinition(String, FieldType);
pub type FieldDefinitions = Vec<FieldDefinition>;

#[derive(Debug, PartialEq, Eq)]
pub enum CreateNode {
    Table(TableName, FieldDefinitions),
    View(ViewName, SelectNode),
    Index(IndexName, FieldName),
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
    lexer: Lexer<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(lexer: Lexer<'a>) -> Self {
        Self { lexer }
    }

    fn parse_varchar_type(&mut self) -> Result<FieldType, String> {
        if self
            .lexer
            .next()
            .map_err(|_| "failed to parse left parenthesis")?
            != Token::LeftParen
        {
            return Err("Did not find expected left parenthesis".to_string());
        }

        return if let Token::IntegerConst(int) = self
            .lexer
            .next()
            .map_err(|_| "failed to parse varchar length")?
        {
            // Read the closing parenthesis
            if Token::RightParen
                != self
                    .lexer
                    .next()
                    .map_err(|_| "failed to parse closing parenthesis")?
            {
                Err("Did not find closing right parenthesis".to_string())
            } else {
                Ok(FieldType::Varchar(int))
            }
        } else {
            Err("Did not find expected varchar length".to_string())
        };
    }

    fn parse_type_def(&mut self) -> Result<FieldType, String> {
        match self
            .lexer
            .next()
            .map_err(|_| "failed to parse field type")?
        {
            Token::Int => Ok(FieldType::Int),
            Token::Varchar => Ok(self.parse_varchar_type()?),
            _ => Err("expected type definition".to_string()),
        }
    }

    fn parse_field_def(&mut self) -> Result<FieldDefinition, String> {
        if let Token::Identifier(field_name) = self
            .lexer
            .next()
            .map_err(|_| "failed to parse field id token")?
        {
            Ok(FieldDefinition(field_name, self.parse_type_def()?))
        } else {
            Err("Failed to parse field name".to_string())
        }
    }

    fn parse_field_defs(&mut self) -> Result<FieldDefinitions, String> {
        match self
            .lexer
            .next()
            .map_err(|_| "failed to parse table name identifier")?
        {
            Token::LeftParen => {
                let mut field_defs: FieldDefinitions = vec![self.parse_field_def()?];

                loop {
                    match self.lexer.next() {
                        Ok(Token::Comma) => field_defs.push(self.parse_field_def()?),
                        Ok(Token::RightParen) => return Ok(field_defs),
                        _ => {
                            return Err(
                                "did not find closing parenthesis of field definitions".to_string()
                            )
                        }
                    }
                }
            }
            _ => Err("Failed to parse left parenthesis in field definitions statement".to_string()),
        }
    }

    fn parse_create_table(&mut self) -> Result<CreateNode, String> {
        if let Token::Identifier(table_name) = self
            .lexer
            .next()
            .map_err(|e| "failed to parse table name identifier")?
        {
            Ok(CreateNode::Table(table_name, self.parse_field_defs()?))
        } else {
            Err("cannot parse CREATE TABLE".to_string())
        }
    }

    fn parse_create_view(&mut self) -> Result<CreateNode, String> {
        if let Ok(Token::Identifier(view_name)) = self.lexer.next()
            && Ok(Token::As) == self.lexer.next()
        {
            Ok(CreateNode::View(view_name, self.parse_select()?))
        } else {
            Err("cannot parse CREATE VIEW".to_string())
        }
    }

    fn parse_create_index(&mut self) -> Result<CreateNode, String> {
        if let Ok(Token::Identifier(index_name)) = self.lexer.next()
            && Ok(Token::On) == self.lexer.next()
            && let Ok(Token::Identifier(field_name)) = self.lexer.next()
        {
            Ok(CreateNode::Index(index_name, field_name))
        } else {
            Err("failed to parse CREATE INDEX statement".to_string())
        }
    }

    fn parse_create(&mut self) -> Result<CreateNode, String> {
        // TODO: handle error
        let typ = self.lexer.next().unwrap();
        match typ {
            Token::Index => self.parse_create_view(),
            Token::Table => self.parse_create_table(),
            Token::View => self.parse_create_view(),
            _ => panic!("TODO: return parse error"),
        }
    }

    fn parse_update(&mut self) -> Result<UpdateNode, String> {
        Err("TODO parse UPDATE".to_string())
    }

    fn parse_delete(&mut self) -> Result<DeleteNode, String> {
        Err("TODO parse DELETE".to_string())
    }

    fn parse_insert(&mut self) -> Result<InsertNode, String> {
        Err("TODO parse INSERT".to_string())
    }

    fn parse_select(&mut self) -> Result<SelectNode, String> {
        Err("TODO parse SELECT".to_string())
    }

    pub fn parse(&mut self) -> Result<RootNode, String> {
        // TODO: return error if the lexer returned an error
        let tok = self.lexer.next().unwrap();
        match tok {
            Token::Create => Ok(RootNode::Create(self.parse_create()?)),
            Token::Update => Ok(RootNode::Update(self.parse_update()?)),
            Token::Delete => Ok(RootNode::Delete(self.parse_delete()?)),
            Token::Insert => Ok(RootNode::Insert(self.parse_insert()?)),
            Token::Select => Ok(RootNode::Select(self.parse_select()?)),
            _ => Err("Failed to parse root statement".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::lexer::Lexer;

    use super::Parser;

    #[test]
    fn test_parse() {
        let text = "CREATE TABLE test ( id int, name varchar(10) )";
        let lexer = Lexer::new(text);
        let mut parser = Parser::new(lexer);

        let ast = parser.parse().unwrap();
        println!("{:?}", ast);
    }
}
