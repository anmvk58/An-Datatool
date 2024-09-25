import os
import jaydebeapi
from src.common.io_utils import IOUtils
from src.connector.Oracle_JDBC_connector import OracleJDBC
from src.connector.Mssql_JDBC_connector import MssqlJDBC
from src.connector.Mysql_JDBC_connector import MysqlJDBC
from src.connector.DB2_JDBC_connector import DB2JDBC
from src.connector.Redshift_JDBC_connector import RedshiftJDBC
from src.common.config_reader import Config_reader
from src.connector.DB2_connector import DB2Connector
from src.connector.Oracle_connector import OracleConnector
from src.connector.Mssql_connector import MssqlConnector
from src.connector.Redshift_connector import RedShiftConnector
from src.common.default_var import DefaultVar

class Connector_factory:

    @staticmethod
    def create_connector(src_name, config_path):
        config = Config_reader(config_path=config_path)
        connection_string = config.get_info_database(src_name=src_name)
        engine = connection_string['engine']

        if engine in ["DB2"]:
            # return DB2Connector(src_name, config_path)
            return DB2JDBC(src_name, config_path)

        elif engine in ["MSSQL"]:
            # return MssqlConnector(src_name, config_path)
            return MssqlJDBC(src_name, config_path)

        elif engine in ["ORACLE"]:
            return OracleConnector(src_name, config_path)
            # return OracleJDBC(src_name, config_path)

        elif engine in ["REDSHIFT"]:
            # return RedShiftConnector(src_name, config_path)
            return RedshiftJDBC(src_name, config_path)

        elif engine in ["MYSQL"]:
            # return RedShiftConnector(src_name, config_path)
            return MysqlJDBC(src_name, config_path)

        else:
            # logger.error("Not founded: engine %s" % engine)
            raise NotImplementedError("Not founded: engine %s" % engine)

    @staticmethod
    def validate_connection(data, engine, password):
        server = data.get("server")
        schema = data.get("schema")
        service_name = data.get("service_name")
        port = data.get("port")
        user = data.get("user")
        DB = data.get("DB")

        list_driver = IOUtils.get_src_path(os.getcwd())

        if engine == 'DB2':
            try:
                jaydebeapi.connect("com.ibm.db2.jcc.DB2Driver",
                                     f"""jdbc:db2://{server}:{port}/{DB}""",
                                     [user, password],
                                     list_driver)

                return True, 'Success'
            except Exception as e:
                return False, e.args[0]
        elif engine == 'ORACLE':
            try:
                jaydebeapi.connect("oracle.jdbc.OracleDriver",
                                   f"""jdbc:oracle:thin:@{server}:{port}/{service_name}""",
                                   [user, password],
                                   list_driver)

                return True, 'Success'
            except Exception as e:
                return False, e.args[0]
        elif engine == 'REDSHIFT':
            try:
                jaydebeapi.connect("com.amazon.redshift.jdbc42.Driver",
                                   f"""jdbc:redshift://{server}:{port}/{DB}""",
                                   [user, password],
                                   list_driver)

                return True, 'Success'
            except Exception as e:
                return False, e.args[0]
        elif engine == 'MSSQL':
            try:
                jaydebeapi.connect("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                                   f"""jdbc:sqlserver://{server}:{port};
                                                      databaseName={schema}""",
                                   [user, password],
                                   list_driver)
                return True, 'Success'
            except Exception as e:
                return False, e.args[0]
        elif engine == 'MYSQL':
            try:
                jaydebeapi.connect("com.mysql.cj.jdbc.Driver",
                                   f"jdbc:mysql://{server}:{port}/{schema}",
                                   [user, password],
                                   list_driver)
                return True, 'Success'
            except Exception as e:
                return False, e.args[0]



if __name__ == '__main__':
    ora_cnn = Connector_factory.create_connector('EFICAZ', config_path=DefaultVar.DEV_ENV )
    # df = ora_cnn.read_sql_query("""
    #     SELECT * FROM VPB_DIH_DB.FCT_AR_DRAWING_ACTY
    # """)
    df2 = ora_cnn.read_sql_query("SELECT CAST('99999999999999999999' AS DECIMAL(20,0)) AS VKL FROM DUAL")
    print("hello")
    print(df2[0])
    # print(pyodbc.drivers())
