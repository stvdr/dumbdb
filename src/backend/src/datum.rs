use crate::page::Page;
use crate::page::ReadTypeFromPage;
use crate::page::WriteTypeToPage;

#[derive(Debug, PartialEq)]
pub enum Datum<'a> {
    Int(i32),
    SmallInt(i16),
    BigInt(i64),
    Real(f32),
    Double(f64),
    Bool(bool),

    // Pointer types
    Varchar(&'a str),
}

//type Datum = u64;
//
//pub struct Datum(u64);

impl Datum<'_> {
    //pub fn as_int(&self) -> i32 {
    //    self.0 as i32
    //}

    //pub fn as_smallint(&self) -> i16 {
    //    self.0 as i16
    //}

    //pub fn as_bigint(&self) -> i64 {
    //    self.0 as i64
    //}

    //pub fn as_bool(&self) -> bool {
    //    self.0 as bool
    //}

    //pub fn as_varchar(&self) -> &str {}

    pub fn from_page_int(p: &Page, offset: usize) -> Datum {
        Datum::Int(p.read(offset))
    }

    pub fn from_page_varchar(p: &Page, offset: usize) -> Datum {
        //p.read_bytes()
        Datum::Varchar(p.read(offset))
    }
}

#[cfg(test)]
mod tests {
    use crate::page::Page;

    use super::Datum;

    #[test]
    fn test_datum_memory_size() {
        assert_eq!(size_of::<Datum>(), size_of::<usize>());
    }

    #[test]
    fn test_datum_int() {
        let mut source = Page::new();
        source.write(256 as i32, 0);
        let dat = Datum::from_page_int(&source, 0);
        assert_eq!(dat, Datum::Int(256));
    }

    #[test]
    fn test_datum_varchar() {
        let mut source = Page::new();
        source.write("okay here is a string", 0);
        let dat = Datum::from_page_varchar(&source, 0);
        assert_eq!(dat, Datum::Varchar("okay here is a string"));
    }
}
