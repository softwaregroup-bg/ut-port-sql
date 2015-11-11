{
  // tutorial: https://coderwall.com/p/316gba/beginning-parsers-with-peg-js
  // online testing: http://pegjs.org/online
   tables = [];
   table = {};
   table.columns = [];
   table.primaryKeys = [];
   table.foreignKeys = [];
   table.uneques = [];
   constraints = {};
   firstCome = false;
   length = 0;

   function addPrimaryKey(key, order, last) {
      var result = {name: key};
      order && (result.order = order);
      if (!last) {
         table.primaryKeys.push(result);
      } else {
         table.primaryKeys.unshift(result);
      }
   }

   function addForeignKey(key, ref, tableName) {
      if (!firstCome) {
         firstCome = true;
         length = table.foreignKeys .push({keys: [], refs: [] });
         length -= 1;
      }
      if (tableName) {
         table.foreignKeys [length].table = tableName;
         table.foreignKeys [length].keys.unshift(key);
         table.foreignKeys [length].refs.unshift(ref);
         firstCome = false;
      } else {
         key && table.foreignKeys [length].keys.push(key);
         ref && table.foreignKeys [length].refs.push(ref);
      }
   }

   function addUneque(name, last) {
      if (!last) {
	     table.uneques.push(name);
 	  }
      else {
		 table.uneques.unshift(name)
	  }
   }
}

start = create_stmt*

create_stmt
   = "CREATE TABLE" ws+
   this_table_name ws* lparen ws*
   table_element ws* (comma ws* element_or_constraint)* ws* with? ws* on? ws* rparen whatever {
      tables.push(table)
      table = {}
      table.columns = []
      table.primaryKeys = []
      table.foreignKeys = [];
      table.uneques = [];
      return tables;
   }

element_or_constraint
   = table_constraint / table_element

table_constraint
   = "CONSTRAINT" ws* column_name ws*
   (tab_const_pk / tab_const_fk / tab_const_unique)

tab_const_unique
   = "UNIQUE" ws* lparen ws* name:column_name ws*
   (comma ws* name:column_name { addUneque(name); })* rparen
   { addUneque(name, true); }

tab_const_pk
   = "PRIMARY KEY" ws+ clust? ws* lparen ws*
   key:column_name ws* order:order? ws*
   (comma ws* key:column_name ws* order:order? { addPrimaryKey(key, order) })* ws* rparen ws*
   { addPrimaryKey(key, order, true) }

on = "ON" ws* column_name

tab_const_fk
   = "FOREIGN KEY" ws* lparen ws* key:column_name ws*
   (comma ws* key:column_name { addForeignKey(key) })* rparen ws*
   "REFERENCES" ws+ tb:table_name ws* lparen ref:column_name ws*
   (comma ws* ref:column_name { addForeignKey(null, ref) })* rparen ws*
   { addForeignKey(key, ref, tb); }

fk_clause_action
   = "ON" ws+ ("DELETE" / "UPDATE" / "INSERT") ws+ ("NO ACTION" / "SET DEFAULT" / "SET NULL" / "CASCADE" / "RESTRICT")

with
   = "WITH" ws* lparen ws* [a-zA-Z_ =,]+ ws* rparen

table_element
   = col_name:column_name ws+
   col_type:colType
   col_constraint:(ws+ column_constraint)* {
      table.columns.push({
         name: col_name,
         type: col_type,
         constraints: constraints
      });
      constraints = {};
   }

this_table_name
   = name:name { table.tableName = name }

table_name
   = name:name { return name; }

name
   = n:[\[A-Za-z_\.\]0-9]+ "]"? { return n.join("")}

column_name
   = col_name:("["? ident+ "]"?) {
   var result = col_name[1].join("");
   if (col_name[0] == "[" && col_name[2] == "]") {
       result = "[" + result + "]";
   }
   return result;
}

colType
   = "["? str:ident+ "]"? size:colTypeSize? {
      var result = {name: str.join("").toLowerCase()};
      size && (result.size = size)
      return result;
   }


colTypeSize
   = "(" i:(integer / ident+) ")" {
      return i.join("")
   }

column_constraint
   = constr_pk
   / constr_default
   / constr_null
   / constr_not_null
   / constr_identity
   / constr_constr

constr_pk = "PRIMARY KEY" ws+ clust? ws+
constr_constr = "CONSTRAINT" ws+ column_name
constr_not_null = "NOT NULL" { constraints.isNull = false}
constr_null = "NULL" { constraints.isNull = true}
constr_identity = "IDENTITY" ws* args:identity_args? {
  constraints.identity = args || "default"
}
constr_unique = "UNIQUE"
constr_default = "DEFAULT" ws+ "("? size:(signed_default_number / literal_value / function_call) ")"? {
  constraints.default = size;
}

// TEST DELETE AFTER
identity_args
   = lparen ws* s:integer ws* "," ws* i:integer ws* rparen {
      return {
         seed: s.join(""),
         increment: i.join("")
      }
   }

signed_default_number
   = str:(("+"/"-")? (integer / float)) {
      return str.join("")
   }

literal_value
  = integer  / float / string
  / "NULL"
  / "CURRENT_TIME"
  / "CURRENT_DATE"
  / "CURRENT_TIMESTAMP"
  / bool

bool = "TRUE" / "FALSE"
order = "ASC" / "DESC"
clust = "CLUSTERED" / "NONCLUSTERED"

quote_single = "'"
quote_double = "\""
string_single = (quote_single string_core quote_single)
string_double = (quote_double string_core quote_double)
string_core = [ a-zA-Z0-9]
string = (string_single / string_double)
whatever = .*
function_call = value:(ident+ lparen rparen)  {
   return value[0].join("") + "()"
}
lparen = "("
rparen = ")"

ws = [ \n\t]
rest = [ a-zA-z\n]+
ident = [A-Za-z_0-9]
comma = ","

integer = [0-9]+
float = [0-9]+ "." [0-9]+
