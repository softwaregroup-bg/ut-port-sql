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
  function print(statement, next) {
    if (!/^END\b/i.test(next?.statement || '')) next = statement;
    return ';PRINT(\'ut-cover '
      + statement.location.start.line + ' '
      + statement.location.start.column + ' '
      + next.location.end.line + ' '
      + next.location.end.column + '\');'
  }
  function instrument(statement, index, array) {
    if (/^END\b/i.test(statement.statement)) return statement.statement;
    let result = statement.statement
    if (/^IF|^ELSE|^WHILE/i.test(statement.original)) {
      const begin = array[index + 1] && !/^BEGIN|^IF|^ELSE|^WHILE/i.test(array[index + 1].statement);
      return begin ? result.substr(0, statement.index) + ' BEGIN' + print(statement, array[index + 1]) + result.substr(statement.index) : result;
    }
    const end = index && /^IF|^ELSE|^WHILE/i.test(array[index-1].original) && !/^BEGIN|^IF|^ELSE|^WHILE/i.test(statement.statement);
    return result.substr(0, statement.index) + print(statement, array[index + 1]) + (end ? 'END;' : '') + result.substr(statement.index);
  }
}

createBody = procedureBody / tableValue / table
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

headerParse = ws createoralter ws1 PROCEDURE ws1 schema:schema table:name doc:WhitespaceSingleLineComment? params:params? AS ws1 {
  return {schema, table, doc, text: text()}
}

procedureBody
  =  header:headerParse body:bodyParse {
      return {
            body: header.text + body.text,
            statements: body.statements,
            type:'procedure',
            name: '['+header.schema+'].['+header.table+']',
            schema: header.schema,
            table: header.table,
            doc: header.doc && header.doc.single.replace(/^\s+/, '').replace(/\s+$/, '') || false,
            params:header.params
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
  = ws createoralter ws1 TABLE ws1 schema:schema table:name ws lparen doc:WhitespaceSingleLineComment? fc:fields_and_constraints ws rparen wsnocomment? options:Comment? ws{
      return {
          type: 'table',
          name: '['+schema+'].['+table+']',
          schema: schema,
          table: table,
          doc: doc && doc.single.replace(/^\s+/, '').replace(/\s+$/, '') || false,
          options: options && parseJSON(options.multi || options.single || ''),
          fields: fc.filter(function(x){return x.isField}),
          indexes: fc.filter(function(x){return x.type === 'INDEX'}),
          constraints: fc.filter(function(x){return x.isConstraint})
      }
    }

name_brackets = "[" str:$[^\]]+ "]" {return str}
name = name_brackets / $[A-Za-z0-9_$]+

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

reference = ns:namespace* n:name {return (ns||'')+'['+n+']'}

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

param = n:param_name ws1 t:param_type d:(ws "=" ws v:value)? o:(ws1 out)? (ws1 "READONLY"i)? {return {name:n, def:t, out:!!o, default:!!d}}
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

default_value = signed_number / string_literal / expression {return null}

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
  = "FOREIGN KEY"i ws lparen ws n:name ws rparen ws "REFERENCES"i ws t:reference ws lparen ws c:name ws rparen a:fk_clause_actions {
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
identity = ws1 "IDENTITY" a:identity_arguments? {return a || {}}
identity_arguments = ws lparen ws s:signed_number ws comma ws i:signed_number ws rparen {return {seed: s, increment: i}}
table_type = n1:name "." n2:name {return {type:'table', typeName:n1+'.'+n2}}

scalar_type =  n:name
  size:( ( ws lparen ws s:(signed_number / "max"i) ws rparen {return s} )
  / ws lparen ws s1:signed_number ws comma ws s2:signed_number ws rparen {return [s1,s2]})? {
    return {type:n.toLowerCase(), size:defaultSize(n.toLowerCase(), size)}
  }

signed_number =
  ( ( plus / minus )? numeric_literal ) {var result = Number.parseFloat(text()); return Number.isNaN(result)?text():result;}

value = lparen ws e:expression ws rparen / signed_number / string_literal / "NULL"i end

string_literal = quote s:([^'] / qq)* quote {return s.join('')}

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

bodyParse =  x: statement+ {
  const result = [];
    x.forEach(item => {
      if (item?.start) {
          result.push(item)
      } else {
        result[result.length-1].statement += item.statement;
        result[result.length-1].location.end = item.location.end;
      }
      if (!item?.whitespace) {
        result[result.length-1].index = result[result.length-1].statement.length;
        if (/@@ROWCOUNT|ROWCOUNT_BIG/i.test(item.statement)) result[result.length-1].rowcount = true;
      }
      if (item?.reference) {
        result[result.length-1].reference||=[];
        if (!result[result.length-1].reference.includes(item.reference)) result[result.length-1].reference.push(item.reference);
      }
    })
    const statements = result.reduceRight((prev, item) => {
       if (
         (prev.length && prev[0].rowcount) ||
         (prev.length && prev[0].statement.startsWith(';')) ||
         (prev.length && prev[0].statement.match(/^(INSERT|UPDATE|DELETE|SET)\b/i) && item.statement.match(/^WHEN\b/i)) ||
         (prev.length && prev[0].statement.match(/^WHEN\b/i) && item.statement.match(/^(MERGE|WITH|INSERT|DELETE|UPDATE|SET)\b/i)) ||
         (prev.length && prev[0].statement.match(/^WITH\b/i) && item.statement.match(/^MERGE\b/i)) ||
         (prev.length && prev[0].statement.match(/^(WITH|UNION)\b/i) && item.statement.match(/^SELECT\b/i)) ||
         (prev.length && prev[0].statement.match(/^(UPDATE|INSERT)\b/i) && item.statement.match(/^WITH\b/i)) ||
         (prev.length && prev[0].statement.match(/^SELECT\b/i) && item.statement.match(/^(INSERT|WITH|UNION|CURSOR)\b/i)) ||
         (prev.length && prev[0].statement.match(/^SET\b/i) && item.statement.match(/^UPDATE\b/i)) ||
         (prev.length && prev[0].statement.match(/^CURSOR\b/i) && item.statement.match(/^DECLARE\b/i))
       ) {
         prev[0].statement = item.statement + prev[0].statement;
         prev[0].index += item.statement.length;
         prev[0].location.start = item.location.start;
         prev[0].rowcount = item.rowcount;
       } else {
         item.original = item.statement;
         prev.unshift(item);
       }
       return prev;
    }, []);
    return {
      text: statements.map(instrument).join('') + '\n',
      statements
    }
}
statement =
  ws1 {return {whitespace: true, statement: text(), location: location()}} /
  value {return {statement: text(), location: location()}} /
  ("(" ws ")") {return {statement: text(), location: location()}} /
  relation: relation {return {statement: text(), location: location(), reference: relation.reference}} /
  name_brackets {return {statement: text(), location: location()}} /
  case {return {statement: text(), location: location()}} /
  start {return {start: true, statement: text(), location: location()}} /
  skip {return {statement: text(), location: location()}}
case = "CASE"i $ws1 $(!("END"i end) (case/(!ws1 .))+ ws1)+ ("END"i end)
relation =
  ("FROM"i / "REFERENCES"i / "JOIN"i / "INTO"i) ws1 reference:reference {return {reference}}
end = &[^A-Za-z0-9_]
start =
  ";" /
  "SET"i end /
  "SELECT"i end /
  "DECLARE"i end /
  "CURSOR"i end /
  "OPEN"i end /
  "FETCH"i ws1 "FROM"i end /
  "CLOSE"i end /
  "DEALLOCATE"i end /
  "BEGIN"i ws1 "TRY"i end /
  "BEGIN"i ws1 "CATCH"i end /
  "BEGIN"i end /
  "END"i ws1 "TRY"i end /
  "END"i ws1 "CATCH"i end /
  "END"i end /
  "ELSE"i (ws1 "IF"i)? end /
  "IF"i end /
  "WHILE"i end /
  "RAISERROR"i end /
  "THROW"i end /
  "RETURN"i end /
  "ROLLBACK"i (ws1 "TRANSACTION"i)? end /
  "COMMIT"i (ws1 "TRANSACTION"i)? end /
  "EXEC"i end /
  "CREATE"i end /
  "DROP"i end /
  "ALTER"i end /
  "DELETE"i end /
  "UPDATE"i end /
  "INSERT"i end /
  "MERGE"i end /
  "WHEN"i end /
  "UNION"i (ws1 "ALL"i)? end /
  "RENAME"i end /
  "WITH"i end /
  "DENY"i end /
  "GRANT"i end /
  "REVOKE"i end /
  "TRUNCATE"i ws1 "TABLE"i end /
  "CREATE"i ws1 "TABLE"i end /
  "DROP"i ws1 "TABLE"i end /
  "DISABLE"i ws1 "TRIGGER"i end /
  "ENABLE"i ws1 "TRIGGER"i end /
  "BULK"i ws1 "INSERT"i end

skip = !start (!(WhiteSpace / LineTerminatorSequence / case / "'" / "(") .)+
