#Tao cau truy van sum cho number
def make_sql_for_number_sum(df, table_name):
    number_sql = 'SELECT '
    for i, col in enumerate(df):
        number_sql += 'Sum(CAST("' + col + '" AS numeric(38, 10))) AS "' + col + '", '
    number_sql += '\'SUM\' As Type_Check FROM ' + table_name
    return number_sql

#Tao cau truy van Max cho number
def make_sql_for_number_max(df, table_name):
    number_sql = 'SELECT '
    for i, col in enumerate(df):
        number_sql += 'Max("' + col + '") AS "' + col + '", '
    number_sql += '\'MAX\' As Type_Check FROM ' + table_name
    return number_sql

#Tao cau truy van Min cho number
def make_sql_for_number_min(df, table_name):
    number_sql = 'SELECT '
    for i, col in enumerate(df):
        number_sql += 'Min("' + col + '") AS "' + col + '", '
    number_sql += '\'MIN\' As Type_Check FROM ' + table_name
    return number_sql

#Tao cau truy van select count table:
def make_sql_select_count(table_name):
    sql = "SELECT COUNT_BIG(*) as total_row FROM " + table_name
    return sql

#Tao cau truy van phuc hop
def make_sql_select_number(list_column, table_name):
    sql = make_sql_for_number_sum(list_column, table_name) + " UNION ALL " + \
          make_sql_for_number_max(list_column, table_name) + " UNION ALL " + \
          make_sql_for_number_min(list_column, table_name)
    return sql