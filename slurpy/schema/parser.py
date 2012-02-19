''' Functions to parse SQL statements.
    Returns a dict with the parsed values.
    Failure returns an emoty dict.
'''
import re
from pyparsing import *

column_stmt = Forward()
create_stmt = Forward()
index_stmt = Forward()
test_stmt = Forward()

LPAR, RPAR, COMMA = map(Suppress, "(),")
(PKEY,NOT,NULL,UNIQUE,DEFAULT, AUTOINCREMENT,CREATE,INDEX,ON) = map(CaselessKeyword, 
     "PRIMARY KEY,NOT,NULL,UNIQUE,DEFAULT,AUTOINCREMENT,CREATE,INDEX,ON".split(","))

# General variables
ident = Word(alphas, alphanums + "_$").setResultsName("identifier")
signed_num = Combine(Optional(oneOf("+-")) + Word(nums + ",."))
order = oneOf("ASC DESC", caseless = True)
temp = oneOf("TEMP TEMPORARY", caseless = True).setResultsName('temp')
ifexists = CaselessKeyword("IF NOT EXISTS").setResultsName("ifexists")
quoted = QuotedString("'")

#CREATE UNIQUE INDEX index_AgSpecialSourceContent_sourceModule ON AgSpecialSourceContent( source, owningModule )

# Tables
table_create = (CaselessKeyword("CREATE") + Optional(temp) + \
                CaselessKeyword("TABLE") + \
                Optional(ifexists))

# Columns
column_type = oneOf("INTEGER TEXT SERIAL UUID VARCHAR", caseless = True). \
                                                     setResultsName('type')
column_size = (LPAR + delimitedList(Word(nums)) + RPAR).setResultsName('size')
column_type_stmt = (column_type + Optional(column_size))

# Column Constraints
primary_key = (PKEY + Optional(order) + Optional(AUTOINCREMENT)).setResultsName('pkey')
null = (Optional(NOT) + NULL).setResultsName('null')
unique = (UNIQUE).setResultsName('unique')
default = (DEFAULT + Optional(signed_num) + Optional(quoted)).setResultsName('default')
column_constraints = (primary_key | null | unique | default)

column_core = (ident + Optional(column_type_stmt) + ZeroOrMore(column_constraints))

column_stmt << (column_core)
create_stmt << (table_create + ident)

index_stmt << (CREATE + Optional(UNIQUE).setResultsName('unique') + INDEX
               + ident.setResultsName('name') + ON 
               + ident.setResultsName('tablename') + LPAR
               + delimitedList(ident).setResultsName('columns') + RPAR)


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

def parse_table_statement(tblstmt):
    ''' Parse an SQL statement used to create a table. '''
    try:
        r = create_stmt.parseString(tblstmt)
        rv = {}
        for k in r.keys():
            if isinstance(r[k], ParseResults):
                rv[k] = r[k].asList()
            else:
                rv[k] = r[k]
        colsRe = re.search(".*\((.*)\)", tblstmt.replace("\n", ''), re.MULTILINE)
        if colsRe:
            rv['columns'] = []
            for col in colsRe.group(1).split(','):
                rv['columns'].append(parse_column_statement(col))
        return rv
    except ParseException:
        return {}

def parse_index_statement(idxstmt):
    ''' Parse an SQL statement used to create an index. '''
    try:
        r = index_stmt.parseString(idxstmt)
        rv = {}
        for k,v in r.items():
            if isinstance(v, ParseResults):
                rv[k] = v.asList()
            else:
                rv[k] = v
        return rv
    except ParseException:
        return {}

