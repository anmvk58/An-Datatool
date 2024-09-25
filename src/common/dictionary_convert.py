
class DictionaryConvert:
    dict_MSSQL = {
        'VARCHAR':          'string()',
        'DATETIME':         'timestamp(\'us\')',
        'NUMERIC':          'decimal128({PRE},{SCALE})',
        'FLOAT':            'float64()',
        'DATE':             'date32()',
        'CHAR':             'string()',
        'BIGINT':           'int64()',
        'NVARCHAR':         'string()',
        'UNIQUEIDENTIFIER': 'string()',
        'INT':              'int32()',
        'DECIMAL':          'decimal128({PRE},{SCALE})',
        'DATETIME2':        'timestamp(\'ms\')'
    }

    dict_DB2 = {
        'BIGINT':           'int64()',
        'CHAR':             'string()',
        'DATE':             'date32()',
        'DECIMAL':          'decimal128({PRE}, {SCALE})',
        'DOUBLE':           'float64()',
        'INTEGER':          'int32()',
        'SMALLINT':         'int32()',
        'VARCHAR':          'string()',
        'VARGRAPHIC':       'string()',
        'TIMESTAMP':        'timestamp(\'us\')',
        'DBCLOB':           'string()',
        'CHARACTER':        'string()'
    }

    dict_ORACLE = {
        'CHAR':             'string()',
        'CLOB':             'string()',
        'DATE':             'date32()',
        'FLOAT':            'float64()',
        'LONG':             'string()',
        'NUMBER':           'float64()',
        'NVARCHAR2':        'string()',
        'TIMESTAMP(0)':     'timestamp(\'us\')',
        'TIMESTAMP(3)':     'timestamp(\'us\')',
        'TIMESTAMP(3) WITH TIME ZONE': 'timestamp(\'us\')',
        'TIMESTAMP(6)':     'timestamp(\'us\')',
        'TIMESTAMP(6) WITH TIME ZONE': 'timestamp(\'us\')',
        'TIMESTAMP(9)':     'timestamp(\'us\')',
        'VARCHAR2':         'string()'
    }
