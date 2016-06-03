create = procedure / tableValue / table

createoralter = "ALTER"i / "CREATE"i

procedure
  = ws createoralter ws1 PROCEDURE ws1 schema:schema table:name doc:WhitespaceSingleLineComment? params:params? AS ws1 body {
      return {
          type:'procedure',
            name: '['+schema+'].['+table+']',
            schema: schema,
            table: table,
            doc: doc && doc.single.replace(/^\s+/, '').replace(/\s+$/, '') || false,
            params:params
        }
    }

tableValue
  = ws createoralter ws1 TYPE ws1 schema:schema table:name ws1 AS ws1 TABLE ws lparen doc:WhitespaceSingleLineComment? fields:fields ws rparen ws{
      return {
          type:'table type',
            name: '['+schema+'].['+table+']',
            schema: schema,
            table: table,
            doc: doc && doc.single.replace(/^\s+/, '').replace(/\s+$/, '') || false,
            fields:fields
        }
    }

table
  = ws createoralter ws1 TABLE ws1 schema:schema table:name ws lparen doc:WhitespaceSingleLineComment? fc:fields_and_constraints ws rparen ws{
      return {
          type: 'table',
            name: '['+schema+'].['+table+']',
            schema: schema,
            table: table,
            doc: doc && doc.single.replace(/^\s+/, '').replace(/\s+$/, '') || false,
            fields: fc.filter(function(x){return x.isField}),
            constraints: fc.filter(function(x){return x.isConstraint})
        }
    }

name =
  "[" str:$[^\]]+ "]" {return str} /
  $[A-Za-z0-9_$]+

names = (ws n:name ws o:order? m:(ws comma ws n:name ws o:order? {
          return {
              name: n,
              order: o
            }
        })*) {
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
  f:ws1 p:param q:(ws comma ws:ws r:param { return [ws, r]})* e:ws1 {

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

field_or_constraint = constraint / field


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

param = n:param_name ws1 t:param_type o:(ws1 "out"i)? d:(ws "=" ws v:value)? (ws1 "READONLY"i)? {return {name:n, def:t, out:!!o, default:!!d}}
param_name = "@" n:name {return n}

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

constraint = "CONSTRAINT" ws1 n:name ws1 c:(pk_constraint / fk_constraint / unique_constraint) {
  c.isConstraint = true;
  c.name = n;
  return c
}

pk_constraint
  = "PRIMARY KEY"i ws c:clustered? ws lparen ws n:names ws o:order? ws rparen {
      return {
          type: "PRIMARY KEY",
            clustered: !!c && c.toLowerCase() === "clustered",
            columns: n,
            order: o
        }
    }

clustered = "CLUSTERED"i / "NONCLUSTERED"i
order = "ASC"i / "DESC"i

fk_constraint
  = "FOREIGN KEY"i ws lparen ws n:name ws rparen ws1 "REFERENCES" ws1 t:fullname ws lparen c:name ws rparen {
      return {
          type: "FOREIGN KEY",
            referenceTable: t,
            referenceColumn: c
        }
    }

unique_constraint
   = "UNIQUE"i ws lparen ws n:names ws rparen {
        return {
          type: "UNIQUE",
            columns: n
        }
   }

param_type = table_type / scalar_type

not_nullable = ws1 x:("NULL"i / "NOT NULL"i) {return x.toLowerCase() === "not null"}
identity = ws1 "IDENTITY" a:identity_arguments? {return a || {}}
identity_arguments = ws lparen ws s:signed_number ws comma ws i:signed_number ws rparen {return {seed: s, increment: i}}
table_type = n1:name "." n2:name {return {type:'table', typeName:n1+'.'+n2}}

scalar_type = ( n:name )
  size:( ( ws lparen ws s:(signed_number / "max"i) ws rparen  ) {return s}
  / ( ws lparen ws s1:signed_number ws comma ws s2:signed_number ws rparen){return [s1,s2]})? {return {type:n.toLowerCase(), size:size}}

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
CREATE =  "CREATE"i
TYPE = "TYPE"i
PROCEDURE = "PROCEDURE"i
AS = "AS"i
TABLE = "TABLE"i
body = .*
lparen = "("
rparen = ")"
plus = "+"
minus = "-"
comma = ","

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