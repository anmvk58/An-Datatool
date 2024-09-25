#Tao cau truy van lay ra 10 ban ghi string cho mssql
def make_sql_for_string_mssql(df, table_name, number_of_rows):
    string_sql = 'SELECT TOP ' + str(number_of_rows) + ' '
    string_order = ''
    for i, col in enumerate(df):
        string_sql += '"' + col + '"'
        string_order += '"' + col + '"'
        if i < df.size - 1:
            string_sql += ', '
            string_order += ', '
    string_sql += ' FROM ' + table_name + ' ORDER BY ' + string_order
    return string_sql

def make_sql_select_not_number(list_column, table_name, number_of_row):
    sql = make_sql_for_string_mssql(list_column, table_name, number_of_row)
    return sql