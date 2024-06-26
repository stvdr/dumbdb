use std::{
    iter::Peekable,
    slice::SliceIndex,
    str::{CharIndices, Chars},
};

use super::token::Token;

#[derive(Debug, PartialEq, Eq)]
pub enum LexerError {
    UnterminatedVarchar(String),
}

pub type LexerResult = Result<Token, LexerError>;

pub struct Lexer<'a> {
    text: &'a str,
    iter: Peekable<CharIndices<'a>>,
    start: usize,
    cur: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(text: &'a str) -> Self {
        let iter = text.char_indices().peekable();

        Self {
            text,
            iter,
            start: 0,
            cur: 0,
        }
    }

    pub fn source(&self) -> &str {
        self.text
    }

    fn advance(&mut self) -> Option<char> {
        let (pos, ch) = self.iter.next()?;
        self.cur = pos + 1;
        Some(ch)
    }

    fn peek(&mut self) -> Option<char> {
        self.iter.peek().map(|(_, c)| c).copied()
    }

    fn get_window(&self, start: usize, end: usize) -> String {
        unsafe { self.text.get_unchecked(start..end).to_string() }
    }

    fn scan_comment(&mut self) -> LexerResult {
        while let Some(ch) = self.peek()
            && ch != '\n'
        {
            self.advance();
        }

        Ok(Token::Whitespace)
    }

    fn scan_dash(&mut self) -> LexerResult {
        if let Some(ch) = self.peek()
            && ch == '-'
        {
            self.scan_comment()
        } else {
            Ok(Token::Minus)
        }
    }

    fn scan_number(&mut self) -> LexerResult {
        while let Some(ch) = self.peek()
            && ch.is_digit(10)
        {
            self.advance();
        }

        Ok(Token::IntegerConst(unsafe {
            self.text
                .get_unchecked(self.start..self.cur)
                .parse::<i32>()
                .expect("failed to parse integer string")
        }))
    }

    fn scan_varchar(&mut self) -> LexerResult {
        while let Some(ch) = self.peek()
            && ch != '\''
        {
            self.advance();
        }

        // advance past the last `'` char
        self.advance().ok_or(LexerError::UnterminatedVarchar(
            self.get_window(self.start, self.cur),
        ))?;

        Ok(Token::VarcharConst(
            self.get_window(self.start + 1, self.cur - 1),
        ))
    }

    fn scan_identifier(&mut self) -> LexerResult {
        while let Some(ch) = self.peek()
            && (ch.is_alphanumeric() || ch == '_')
        {
            self.advance();
        }

        let val = self.get_window(self.start, self.cur);

        // TODO: less repetition?
        let token = match val.to_lowercase().as_str() {
            "and" => Token::And,
            "as" => Token::As,
            "create" => Token::Create,
            "delete" => Token::Delete,
            "from" => Token::From,
            "index" => Token::Index,
            "insert" => Token::Insert,
            "int" => Token::Int,
            "into" => Token::Into,
            "on" => Token::On,
            "select" => Token::Select,
            "set" => Token::Set,
            "table" => Token::Table,
            "update" => Token::Update,
            "values" => Token::Values,
            "varchar" => Token::Varchar,
            "view" => Token::View,
            "where" => Token::Where,
            _ => Token::Identifier(val),
        };

        Ok(token)
    }

    fn scan_token(&mut self) -> LexerResult {
        let ch = self.advance();
        match ch {
            Some(' ') | Some('\r') | Some('\t') | Some('\n') => Ok(Token::Whitespace),
            Some('=') => Ok(Token::Equal),
            Some(',') => Ok(Token::Comma),
            Some(';') => Ok(Token::SemiColon),
            Some('-') => self.scan_dash(),
            Some('(') => Ok(Token::LeftParen),
            Some(')') => Ok(Token::RightParen),
            Some('\'') => self.scan_varchar(),
            Some(ch) if ch.is_digit(10) => self.scan_number(),
            Some(ch) if ch.is_alphabetic() => self.scan_identifier(),
            _ => Ok(Token::EOF),
        }
    }
}

impl<'a> Iterator for Lexer<'a> {
    type Item = LexerResult;

    fn next(&mut self) -> Option<Self::Item> {
        let mut tok = self.scan_token();
        self.start = self.cur;

        while let Ok(t) = &tok
            && *t == Token::Whitespace
        {
            tok = self.scan_token();
            self.start = self.cur;
        }

        if tok == Ok(Token::EOF) {
            None
        } else {
            Some(tok)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::{lexer::LexerError, token::Token};

    use super::Lexer;

    macro_rules! lexer_tests {
        ($($name:ident: $input:expr => $expected:expr,)*) => {
            $(
                #[test]
                fn $name() {
                    let mut lex = Lexer::new($input);
                    for tok in $expected {
                        if let Some(actual) = lex.next() {
                            assert_eq!(tok, actual);
                        }
                        else {
                            panic!("unexpected end of lexer input")
                        }
                    }

                    assert_eq!(lex.next(), None)
                }
            )*
        }
    }

    lexer_tests! {
        lexer_identifier_1: "okay" => vec![Ok(Token::Identifier("okay".to_string()))],
        lexer_identifier_2: "test_snake_case" => vec![Ok(Token::Identifier("test_snake_case".to_string()))],

        lexer_integer_1: "1234" => vec![Ok(Token::IntegerConst(1234))],
        lexer_integer_2: "1234,5678" => vec![Ok(Token::IntegerConst(1234)), Ok(Token::Comma), Ok(Token::IntegerConst(5678))],
        lexer_integer_3: "1234, 5678" => vec![Ok(Token::IntegerConst(1234)), Ok(Token::Comma), Ok(Token::IntegerConst(5678))],
        lexer_integer_4: "1234 , 5678" => vec![Ok(Token::IntegerConst(1234)), Ok(Token::Comma), Ok(Token::IntegerConst(5678))],

        lexer_varchar_1: "'hello'" => vec![Ok(Token::VarcharConst("hello".to_string()))],
        lexer_varchar_2: "123'hello'456" => vec![Ok(Token::IntegerConst(123)), Ok(Token::VarcharConst("hello".to_string())), Ok(Token::IntegerConst(456))],
        lexer_varchar_3: "123 'hello' 456" => vec![Ok(Token::IntegerConst(123)), Ok(Token::VarcharConst("hello".to_string())), Ok(Token::IntegerConst(456))],
        lexer_varchar_4: "'abc" => vec![Err(LexerError::UnterminatedVarchar("'abc".to_string()))],
        lexer_varchar_5: "''" => vec![Ok(Token::VarcharConst("".to_string()))],

        lexer_query_1: "SELECT a FROM x, z WHERE b = 3 AND c = 'hello';" => vec![
            Ok(Token::Select),
            Ok(Token::Identifier("a".to_string())),
            Ok(Token::From),
            Ok(Token::Identifier("x".to_string())),
            Ok(Token::Comma),
            Ok(Token::Identifier("z".to_string())),
            Ok(Token::Where),
            Ok(Token::Identifier("b".to_string())),
            Ok(Token::Equal),
            Ok(Token::IntegerConst(3)),
            Ok(Token::And),
            Ok(Token::Identifier("c".to_string())),
            Ok(Token::Equal),
            Ok(Token::VarcharConst("hello".to_string())),
            Ok(Token::SemiColon),
        ],

        lexer_predicate_1: "Dname = 'math' AND GradYear = SName" => vec![
            Ok(Token::Identifier("Dname".to_string())),
            Ok(Token::Equal),
            Ok(Token::VarcharConst("math".to_string())),
            Ok(Token::And),
            Ok(Token::Identifier("GradYear".to_string())),
            Ok(Token::Equal),
            Ok(Token::Identifier("SName".to_string())),
        ],

        lexer_comment_1: "1234 -- a comment\n --another comment \n 5678 \n --another!\n\n 9" => vec![
            Ok(Token::IntegerConst(1234)),
            Ok(Token::IntegerConst(5678)),
            Ok(Token::IntegerConst(9)),
        ],

        lexer_create_table_1: "CREATE TABLE test ( id int, name varchar(10) )" => vec![
            Ok(Token::Create),
            Ok(Token::Table),
            Ok(Token::Identifier("test".to_string())),
            Ok(Token::LeftParen),
            Ok(Token::Identifier("id".to_string())),
            Ok(Token::Int),
            Ok(Token::Comma),
            Ok(Token::Identifier("name".to_string())),
            Ok(Token::Varchar),
            Ok(Token::LeftParen),
            Ok(Token::IntegerConst(10)),
            Ok(Token::RightParen),
            Ok(Token::RightParen),
        ],
    }
}
