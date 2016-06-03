default = lparen lparen v:default_value rparen rparen {return v} /
          lparen v:default_value rparen {return v}

default_value = signed_number / string_literal

signed_number =
  ( ( plus / minus )? numeric_literal ) {var result = Number.parseFloat(text()); return Number.isNaN(result)?text():result;}

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
plus = "+"
minus = "-"
lparen = "("
rparen = ")"