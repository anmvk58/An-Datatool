iưmport ibm_db_dbi as db2
import pandas as pd
from src.connector.connector_factory import Connector_factory
from src.common.default_var import DefaultVar

if __name__ == '__main__':
    # cnn_s16 = Connector_factory.create_connector("WHR2", config_path=DefaultVar.DEV_ENV)
    # cnn_ora_new = Connector_factory.create_connector("EFICAZ", config_path=DefaultVar.DEV_ENV)
    cnn_db2 = Connector_factory.create_connector("VPB_STAG_OTHER", config_path=DefaultVar.DEV_ENV)
    sql_db2 = """
           SELECT * FROM BID_DEV_STAG_OTHERS.ANMV_TEST_COMPARE
       """
    # cursor = cnn_db2['VPB_STAG_OTHER'].create_cursor()
    # q = cursor.execute(sql_db2).fetchone()
    cursor = cnn_db2.create_cursor()
    q = cursor.execute(sql_db2)

    list = cursor.description

    meta_data2 = pd.DataFrame(list)
    headers = ["COL_NAME", "DATA_TYPE", "X", "LENGTH", "PRECISION", "SCALE", "Y"]
    meta_data2.columns = headers
    meta_data2["DATA_TYPE"] = meta_data2.apply(
        lambda x: x["DATA_TYPE"].col_types[0].lower(), axis=1
    )
    print(meta_data2)
