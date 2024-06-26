# Parser

This module contains code necessary for lexing / parsing SQL statements.

## Grammar

```
<Field>         := Token::Identifier
<Constant>      := Token::VarcharConst | Token::IntegerConst
<Expression>    := <Field> | <Constant>
<Term>          := <Expression> = <Expression>
<Predicate>     := <Term> [ AND <Predicate> ]

<Query>         := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
<SelectList>    := <Field> [ , <SelectList> ]
<TableList>     := Token::Identifier [ , <TableList> ]

<UpdateCmd> := <Insert> | <Delete> | <Update> | <Create>

<Insert>    := INSERT INTO Token::Identifier ( <FieldList> ) VALUES ( <ConstList> )
<FieldList> := <Field> [ , <FieldList> ]
<ConstList> := <Constant> [ , <ConstList> ]

<Delete> := DELETE FROM Token::Identifier [ WHERE <Predicate> ]

<Update> := UPDATE Token::Identifier SET <Field> = <Expression> [ WHERE <Predicate> ]

<Create>        := <CreateTable> | <CreateView> | <CreateIndex>

<CreateTable>   := CREATE TABLE Token::Identifier ( <FieldDefs> )
<FieldDefs>     := <FieldDef> [ , <FieldDefs> ]
<FieldDef>      := Token::Identifier <TypeDef>
<TypeDef>       := INT | VARCHAR ( Token::IntegerConst )

<CreateView>    := CREATE VIEW Token::Identifer AS <Query>
<CreateIndex>   := CREATE INDEX Token::Identifier ON Token::Identifier ( <Field> )
```
