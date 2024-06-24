use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq)]
pub enum Token {
    IntegerConst(i32),
    VarcharConst(String),
    Identifier(String),

    Comma,
    SemiColon,
    Whitespace,
    EOF,

    LeftParen,
    RightParen,
    Minus,
    Plus,
    Splat,
    ForwardSlash,
    Equal,

    // Keywords
    And,
    As,
    Create,
    Delete,
    From,
    Index,
    Insert,
    Int,
    Into,
    On,
    Select,
    Set,
    Table,
    Update,
    Values,
    Varchar,
    View,
    Where,
}

//const token_names: HashMap<String, fn(String) -> Token> = HashMap
