from src.common.io_utils import IOUtils
from src.common.default_var import DefaultVar
import json
import pyodbc

def write_json(new_data, filename):
    with open(filename,'r+') as file:
        file_data = json.load(file)
        file_data.append(new_data)
        file.seek(0)
        json.dump(file_data, file, indent = 4)

if __name__ == '__main__':
    file = open(IOUtils.get_absolute_path(DefaultVar.DEV_ENV), 'r+')
    config = json.load(file)

    new_data = {
            "driver": "ODBC Driver 17 for SQL Server",
            "schema": "UPG_WHR2",
            "schema_full": "UPG_WHR2.dbo",
            "server": "10.16.27.55",
            "user": "etl_viewer",
            "password": "gAAAAABjgeH-PbtbAYr60kMFEUdsY_VTDT9V8WyaV5OVrulsPUMtEZDkS7WAwtdXA0ulMzRS9B19kdIoGn-MCjLuQEUMnLeXHw==",
            "port": "1433",
            "engine": "MSSQL"
    }

    config["TEST"] = new_data
    file.seek(0)
    json.dump(config, file, indent=4)
