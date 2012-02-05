''' Functions to parse SQL statements.
    Returns a dict with the parsed values.
    Failure returns an emoty dict.
'''

from pyparsing import *

column_stmt = Forward()
test_stmt = Forward()

LPAR, RPAR, COMMA = map(Suppress, "(),")
(PKEY,NOT,NULL,UNIQUE,DEFAULT, AUTOINCREMENT) = map(CaselessKeyword, 
                          "PRIMARY KEY,NOT,NULL,UNIQUE,DEFAULT,AUTOINCREMENT".split(","))

ident = Word(alphas, alphanums + "_$").setResultsName("identifier")
signed_num = Combine(Optional(oneOf("+-")) + Word(nums + ",."))
order = oneOf("ASC DESC", caseless = True)
quoted = QuotedString("'")
column_name = Upcase(delimitedList(ident, ".", combine=True))

column_type = oneOf("INTEGER TEXT SERIAL UUID VARCHAR", caseless = True). \
                                                     setResultsName('type')
column_size = (LPAR + delimitedList(Word(nums)) + RPAR).setResultsName('size')
column_type_stmt = (column_type + Optional(column_size))

primary_key = (PKEY + Optional(order) + Optional(AUTOINCREMENT)).setResultsName('pkey')
null = (Optional(NOT) + NULL).setResultsName('null')
unique = (UNIQUE).setResultsName('unique')
default = (DEFAULT + Optional(signed_num) + Optional(quoted)).setResultsName('default')

column_constraints = (primary_key | null | unique | default)

column_stmt << (ident + Optional(column_type_stmt) + ZeroOrMore(column_constraints))

def parse_column_statement(colstmt):
    ''' Parse an SQL statement used to create a column. '''
    try:
        r = column_stmt.parseString(colstmt)
        rv = {}
        for k in r.keys():
            if isinstance(r[k], ParseResults):
                rv[k] = r[k].asList()
            else:
                rv[k] = r[k]
        return rv
    except ParseException:
        return {}

