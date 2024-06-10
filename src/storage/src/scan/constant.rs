use std::fmt::Display;

// TODO: more research on ordering
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Constant {
    Integer(i32),
    Varchar(String),
}

impl Display for Constant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(i) => write!(f, "{}", i),
            Self::Varchar(s) => write!(f, "{}", s),
        }
    }
}
