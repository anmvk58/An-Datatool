import pyodbc
import re
import pandas as pd
from src.connector.connector import Connector
from src.common.dictionary_convert import DictionaryConvert

class MysqlConnector(Connector):
    @property
    def driver(self) -> str:
        list_driver = pyodbc.drivers()
        for driver in list_driver:
            if 'MySQL' in driver and 'Unicode' in driver:
                return driver
        raise Exception("Không có driver thích hợp !!!")


    def create_connection(self):
        return  pyodbc.connect("DRIVER={%s}; SERVER=%s; PORT=%s;DATABASE=%s; UID=%s; PASSWORD=%s;" % (
            self.driver, self.server, self.port, self.schema, self.user, self.pw))


    def make_query_limit_1(self, query) -> str:
        query = re.sub(' +', ' ', query.strip())
        first_word = query.split(" ")[0]
        second_word = query.split(" ")[-2]
        third_word = query.split(" ")[-1]
        if first_word.lower() == 'select' and second_word.lower() != 'limit':
            return "SELECT" + query[6:] + " limit 1"
        elif first_word.lower() == 'select' and second_word.lower() == 'limit':
            return query.replace(third_word, '1')
        else:
            return "SELECT * FROM " + query + " LIMIT 1"

if __name__ == '__main__':
    mysql = MysqlConnector('aws_uat_vpb_card', 'connection_defi.json')