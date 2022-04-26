{
  function defaultSize(type, size) {
    switch (type) {
      case 'numeric':
      case 'decimal':
        if (size == null) return [18, 0];
        else if (Array.isArray(size)) return size;
        else return [size, 0];
      case 'datetime2':
      case 'time':
      case 'datetimeoffset': return (size == null) ? 7 : size;
      case 'varchar':
      case 'varbinary':
      case 'char':
      case 'binary': return (size == null) ? 1 : size;
      default:
        return size
    }
  }
  function parseJSON(text) {
    return /(^\{.*}$)|(^\[.*]$)/s.test(text.trim()) ? JSON.parse(text) : text;
  }
}

create = procedure / tableValue / table

createoralter = "ALTER"i / "CREATE"i ws1 "OR"i ws1 "REPLACE"i / "CREATE"i

procedure
  = ws createoralter ws1 PROCEDURE ws1 table:name doc:WhitespaceSingleLineComment? params:params? AS ws1 body {
      return {
          type:'procedure',
            name: table,
            schema: table.split('.')[0],
            table: table.split('.').slice(1).join('.'),
            doc: doc && doc.single.replace(/^\s+/, '').replace(/\s+$/, '') || false,
            params:params
        }
    }

tableValue
  = ws TYPE ws1 tableT:name ws1 IS ws1 TABLE ws OF ws1 table:name "%ROWTYPE" doc:WhitespaceSingleLineComment? ws ";"{
      return {
          type:'table type',
          name: table.split('.')[0] + '.' + tableT,
          schema: table.split('.')[0],
          table: table.split('.').slice(1).join('.'),
          doc: doc && doc.single.replace(/^\s+/, '').replace(/\s+$/, '') || false,
          fields:[]
      }
    }

table
  = ws createoralter ws1 TABLE ws1 table:name ws lparen doc:WhitespaceSingleLineComment? fc:fields_and_constraints ws rparen wsnocomment? options:Comment? ws{
      return {
          type: 'table',
          name: table,
          schema: table.split('.')[0],
          table: table.split('.').slice(1).join('.'),
          doc: doc && doc.single.replace(/^\s+/, '').replace(/\s+$/, '') || false,
          options: options && parseJSON(options.multi || options.single || ''),
          fields: fc.filter(function(x){return x.isField}),
          indexes: fc.filter(function(x){return x.type === 'INDEX'}),
          constraints: fc.filter(function(x){return x.isConstraint})
      }
    }

name =
  '"' str:$[^"]+ '"' {return str} /
  $[A-Za-z0-9_$]+

names = ws n:name ws o:order? m:(ws comma ws nn:name ws oo:order? {
          return {
              name: nn,
              order: oo
            }
        })* {
          m.unshift({
              name: n,
                order: o
            });
            return m
        }

schema = name:name "." {return name}
namespace = name:name "." {return '['+name+'].'}

fullname = ns:namespace? n:name? {return (ns||'')+'['+n+']'}

params =
  f:ws1 lparen ff:ws1 p:param q:(ws comma ws:ws r:param { return [ws, r]})* e:ws1 rparen ee:ws1 {

  var x=[p];
  q.forEach(function(pp){
    x.push(pp[0]);
    x.push(pp[1]);
  });
  x.push(e)
  var lastParam;
  var lastComment=f[0] && f[f.length-1].multi;
  var y = [];
  x.forEach(function(pp, i){
    if (i % 2 === 0) {
       lastParam = pp;
       lastComment && (pp.doc = lastComment.replace(/^\s+/, '').replace(/\s+$/, '')) && (lastComment=null);
       y.push(pp);
    } else {
       if (pp.length && pp[0]){
         pp[0].single && !lastParam.doc && (lastParam.doc = pp[0].single.replace(/^\s+/, '').replace(/\s+$/, ''));
         lastComment = pp[pp.length-1].multi;
         pp[0].multi && (lastComment = pp[0].multi);
       }
    }
  });
  return y

} / ws1

fields_and_constraints = f:ws p:field_or_constraint q:(ws comma ws:ws r:field_or_constraint { return [ws, r]})* e:ws1 {

  var x=[p];
  q.forEach(function(pp){
    x.push(pp[0]);
    x.push(pp[1]);
  });
  x.push(e)
  var lastParam;
  var lastComment=f[0] && f[f.length-1].multi;
  var y = [];
  x.forEach(function(pp, i){
    if (i % 2 === 0) {
       lastParam = pp;
       lastComment && (pp.doc = lastComment.replace(/^\s+/, '').replace(/\s+$/, '')) && (lastComment=null);
       y.push(pp);
    } else {
       if (pp.length && pp[0]){
         pp[0].single && !lastParam.doc && (lastParam.doc = pp[0].single.replace(/^\s+/, '').replace(/\s+$/, ''));
         lastComment = pp[pp.length-1].multi;
         pp[0].multi && (lastComment = pp[0].multi);
       }
    }
  });
  return y


}

field_or_constraint = index / constraint / field


fields =
  f:ws p:field q:(ws comma ws:ws r:field {return [ws, r]})* e:ws {
  var x=[p];
  q.forEach(function(pp){
    x.push(pp[0]);
    x.push(pp[1]);
  });
  x.push(e)
  var lastParam;
  var lastComment=f[0] && f[f.length-1].multi;
  var y = [];
  x.forEach(function(pp, i){
    if (i % 2 === 0) {
       lastParam = pp;
       lastComment && (pp.doc = lastComment.replace(/^\s+/, '').replace(/\s+$/, '')) && (lastComment=null);
       y.push(pp);
    } else {
       if (pp.length && pp[0]){
         pp[0].single && !lastParam.doc && (lastParam.doc = pp[0].single.replace(/^\s+/, '').replace(/\s+$/, ''));
         lastComment = pp[pp.length-1].multi;
         pp[0].multi && (lastComment = pp[0].multi);
       }
    }
  });
  return y

  }

mode = "IN"i ws1 "OUT"i {return {dir: 'inout'}} / "IN"i {return {dir: 'in'}} / "OUT"i {return {dir: 'out'}}
param = n:param_name ws1 mode:mode? ws t:param_type d:(ws ("DEFAULT"i / ":=") ws v:value)? {return {name:n, def:{...t, ...mode}, default:!!d}}
param_name = n:name {return n}

field = n:name ws1 t:param_type i:identity? not_nullable:not_nullable? d:default? {
  var identity = !!i;
    var result = {
      column:n,
      type:t.type,
      nullable:!not_nullable,
      length:Array.isArray(t.size) ? t.size[0] : t.size,
      scale:Array.isArray(t.size) ? t.size[1] : null,
      identity: i || false,
      isField:true,
      default:d
    }
    return result
}

default = ws "DEFAULT"i ws lparen ws v:default_value ws rparen {return v}

default_value = signed_number / string_literal

constraint = "CONSTRAINT" ws1 n:name ws1 c:(pk_constraint / fk_constraint / unique_constraint / check_constraint) {
  c.isConstraint = true;
  c.name = n;
  return c
}

pk_constraint
  = "PRIMARY KEY"i ws c:clustered? ws lparen ws n:names ws rparen {
      return {
          type: "PRIMARY KEY",
          clustered: !!c && c.toLowerCase() === "clustered",
          columns: n
        }
    }

clustered = "CLUSTERED"i / "NONCLUSTERED"i
order = "ASC"i / "DESC"i
action = "NO ACTION"i / "SET DEFAULT"i / "SET NULL"i / "CASCADE"i / "RESTRICT"i
clause = "DELETE"i / "UPDATE"i / "INSERT"i

fk_constraint
  = "FOREIGN KEY"i ws lparen ws n:name ws rparen ws "REFERENCES"i ws t:fullname ws lparen ws c:name ws rparen a:fk_clause_actions {
      return {
          type: "FOREIGN KEY",
            referenceTable: t,
            referenceColumn: c,
            actionClauses: a
        }
    }

fk_clause_action
   = "ON"i ws c:clause ws a:action {
     return {
       type: "CLAUSE_ACTION",
       clause: c,
       action: a
     }
   }

fk_clause_actions = q:(w:ws ac:fk_clause_action {return [w, ac]})* {
    var y = [];
    q.forEach(function(pp){
      y.push(pp[1]);
    });
    return y;
  }

unique_constraint
   = "UNIQUE"i ws c:clustered? ws lparen ws n:names ws rparen {
        return {
          type: "UNIQUE",
            columns: n
        }
   }

check_constraint
   = "CHECK"i ws lparen ws e:expression ws rparen {
        return {
          type: "CHECK",
            expression: e
        }
   }

expression = term (ws term ws)* {return text()}
term = [^()]+ / "(" expression? ")"

param_type = table_type / scalar_type

not_nullable = ws x:("NULL"i / "NOT NULL"i) {return x.toLowerCase() === "not null"}
identity = ws1 "GENERATED"i ws1 "BY"i ws1 "DEFAULT"i ws1 "AS"i ws1 "IDENTITY"i a:identity_arguments? {return a || {}}
identity_arguments = ws lparen s:identity_start i:identity_increment? ws rparen {return {seed: s, increment: i}}
identity_start = ws "START"i ws1 "WITH"i ws1 s:signed_number {return s}
identity_increment = ws "INCREMENT"i ws1 "BY"i ws1 i:signed_number {return i}
table_type = n1:name "." n2:name {return {type:'nested', typeName:n1+'.'+n2}}

scalar_type =  n:name
  size:( ( ws lparen ws s:(signed_number / "max"i) ws rparen {return s} )
  / ws lparen ws s1:signed_number ws comma ws s2:signed_number ws rparen {return [s1,s2]})? {
    return {type:n.toLowerCase(), size:defaultSize(n.toLowerCase(), size)}
  }

signed_number =
  ( ( plus / minus )? numeric_literal ) {var result = Number.parseFloat(text()); return Number.isNaN(result)?text():result;}

value = numeric_literal / string_literal / "NULL"i

string_literal = quote s:([^'\r\n] / qq)* quote {return s.join('')}

qq = quote quote {return '\''}

numeric_literal =
  digits:( ( ( ( digit )+ ( decimal_point ( digit )+ )? )
           / ( decimal_point ( digit )+ ) )
           ( E ( plus / minus )? ( digit )+ )? )

digit = [0-9]
quote = "'"
decimal_point = "."
E = "E"i
ws = ws:(WhiteSpace {return } / LineTerminatorSequence {return } / Comment)* {return ws.filter(function(x){return x})}
ws1 = ws:(WhiteSpace {return } / LineTerminatorSequence {return } / Comment)+ {return ws.filter(function(x){return x})}
wsnocomment = ws:(WhiteSpace {return } / LineTerminatorSequence {return })* {return ws.filter(function(x){return x})}
CREATE =  "CREATE"i
TYPE = "TYPE"i
PROCEDURE = "PROCEDURE"i
AS = "AS"i
IS = "IS"i
OF = "OF"i
TABLE = "TABLE"i
body = .*
lparen = "("
rparen = ")"
plus = "+"
minus = "-"
comma = ","

out = "output"i / "out"i

Zs = [\u0020\u00A0\u1680\u2000-\u200A\u202F\u205F\u3000]

SourceCharacter = .

WhiteSpace "whitespace"
  = "\t"
  / "\v"
  / "\f"
  / " "
  / "\u00A0"
  / "\uFEFF"
  / Zs

LineTerminator = [\n\r\u2028\u2029]

LineTerminatorSequence "end of line"
  = "\n"
  / "\r\n"
  / "\r"
  / "\u2028"
  / "\u2029"

Comment "comment"
  = MultiLineComment
  / SingleLineComment

MultiLineCommentBody =
  (!"*/" SourceCharacter)*{return {multi:text()}}

MultiLineComment
  = "/*" x:MultiLineCommentBody "*/" {return x}

MultiLineCommentNoLineTerminator
  = "/*" (!("*/" / LineTerminator) SourceCharacter)* "*/"

SingleLineCommentBody = (!LineTerminator SourceCharacter)* {return {single:text()}}
SingleLineComment
  = "--" x:SingleLineCommentBody {return x}
WhitespaceSingleLineComment = WhiteSpace? "--" x:SingleLineCommentBody {return x}

unique = "UNIQUE"i

index
  = "INDEX"i ws1 n:name ws u:unique? ws c:clustered? ws lparen ws col:names ws rparen
    filter:(ws1 "WHERE" filter:((ws1 name)+ {return text().trim()}){return filter})? {
      return {
          type: "INDEX",
          name: n,
          clustered: !!c && c.toLowerCase() === "clustered",
          unique: !!u && u.toLowerCase() === "unique",
          columns: col,
          filter
        }
    }
