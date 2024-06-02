use std::collections::HashMap;

#[derive(Clone)]
struct FieldInfo {
    typ: i32,
    length: u64,
}

impl FieldInfo {
    pub fn new(typ: i32, length: u64) -> Self {
        Self { typ, length }
    }
}

#[derive(Clone)]
pub struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            fields: vec![],
            info: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, name: &str, typ: i32, length: u64) -> &mut Self {
        assert!(!self.fields.contains(&name.to_string()));
        self.fields.push(name.to_string());
        self.info
            .insert(name.to_string(), FieldInfo::new(typ, length));
        self
    }

    pub fn add_int_field(&mut self, name: &str) -> &mut Self {
        self.add_field(name, 0, 0);
        self
    }

    pub fn add_string_field(&mut self, name: &str, length: u64) {
        self.add_field(name, 1, length);
    }

    pub fn add(&mut self, name: &str, sch: &Schema) {
        let typ = sch
            .get_field_type(&name)
            .expect(&format!("schema has no field named '{}'", name));
        let len = sch
            .get_field_length(&name)
            .expect(&format!("schema has no field named '{}'", name));

        self.add_field(name, typ, len);
    }

    pub fn add_all(&mut self, sch: &Schema) {
        for field_name in sch.fields.iter() {
            self.add(field_name, sch);
        }
    }

    pub fn get_field_type(&self, name: &str) -> Option<i32> {
        self.info.get(name).map(|f| f.typ)
    }

    pub fn get_field_length(&self, name: &str) -> Option<u64> {
        self.info.get(name).map(|f| f.length)
    }

    pub fn fields(&self) -> Vec<String> {
        self.fields.clone()
    }

    pub fn has_field(&self, name: &str) -> bool {
        self.fields.contains(&name.to_string())
    }
}
