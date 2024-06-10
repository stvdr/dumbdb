use super::{
    constant::Constant,
    scan::{Scan, ScanResult},
};

struct ProductScan<'a> {
    left: &'a mut dyn Scan,
    right: &'a mut dyn Scan,
}

impl<'a> ProductScan<'a> {
    pub fn new(left: &'a mut dyn Scan, right: &'a mut dyn Scan) -> Self {
        Self { left, right }
    }
}

impl Scan for ProductScan<'_> {
    fn before_first(&mut self) {
        self.left.before_first();
        self.left.next();
        self.right.before_first();
    }

    fn next(&mut self) -> bool {
        let records_exist_in_right = self.right.next();
        if records_exist_in_right {
            true
        } else {
            self.right.before_first();
            self.left.next()
        }
    }

    fn get_int(&self, field_name: &str) -> ScanResult<i32> {
        if self.left.has_field(field_name) {
            self.left.get_int(field_name)
        } else {
            self.right.get_int(field_name)
        }
    }

    fn get_string(&self, field_name: &str) -> ScanResult<String> {
        if self.left.has_field(field_name) {
            self.left.get_string(field_name)
        } else {
            self.right.get_string(field_name)
        }
    }

    fn get_val(&self, field_name: &str) -> ScanResult<Constant> {
        if self.left.has_field(field_name) {
            self.left.get_val(field_name)
        } else {
            self.right.get_val(field_name)
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.left.has_field(field_name) || self.right.has_field(field_name)
    }

    fn close(&mut self) {
        self.left.close();
        self.right.close();
    }
}
