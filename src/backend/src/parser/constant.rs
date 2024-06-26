use std::fmt::Display;

// TODO: more research on ordering
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Int(i32),
    Varchar(String),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(i) => write!(f, "{}", i),
            Self::Varchar(s) => write!(f, "'{}'", s),
        }
    }
}

pub trait FromDynamic {
    fn as_val(&self) -> Value;
}

impl FromDynamic for i32 {
    fn as_val(&self) -> Value {
        Value::Int(*self)
    }
}

impl FromDynamic for &str {
    fn as_val(&self) -> Value {
        Value::Varchar(self.to_string())
    }
}

impl FromDynamic for String {
    fn as_val(&self) -> Value {
        Value::Varchar(self.to_string())
    }
}
