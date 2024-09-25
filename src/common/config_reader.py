import json
from src.common.io_utils import IOUtils

class Config_reader:
    config = {}

    def __init__(self, config_path='connection_defi.json'):
        try:
            file = open(IOUtils.get_absolute_path(config_path), 'r')
            self.config = json.load(file)
        except FileNotFoundError:
            with open(config_path, 'w') as json_file:
                json.dump({}, json_file)
            self.config = {}
        finally:
            self.config_path = config_path


    def get_info_database(self, src_name = 'VPB_STAG_OTHER'):
        return self.config[src_name]

    def get_list_src_name_original(self):
        list_src = []
        file = open(IOUtils.get_absolute_path(self.config_path), 'r')
        config = json.load(file)
        for item in config:
            list_src.append(item)
        return list_src

    def get_list_src_name(self):
        list_src = []
        # Reload file again
        file = open(IOUtils.get_absolute_path(self.config_path), 'r')
        config = json.load(file)
        for item in config:
            if(config[item]['engine'] == 'DB2'):
                val = config[item]['engine']+ '          |  ' + self.print_host(config[item]['server']) + ' |  ' + item
            elif(config[item]['engine'] == 'MSSQL'):
                val = config[item]['engine'] + '    |  ' + self.print_host(config[item]['server']) + ' |  ' + item
            elif(config[item]['engine'] == 'ORACLE'):
                val = config[item]['engine'] + '  |  ' + self.print_host(config[item]['server']) + ' |  ' + item
            else:
                val = config[item]['engine'] + '  |  ' + self.print_host(config[item]['server']) + ' |  ' + item
            list_src.append(val)
        list_src.sort()
        return list_src

    def convert_to_src_name(self, full_src_name):
        list = full_src_name.split("|")
        return list[-1].strip()

    def print_host(self, string):
        if len(string) > 15 :
            string = string[0:13] + '...'

        lg_str = 14 - len(string)
        if(lg_str == 1):
            return string
        elif(lg_str == 2):
            return string + ' '
        elif (lg_str == 3):
            return string + '   '
        else:
            return string + '   '
# main only for testing unit

if __name__ == '__main__':
    config_reader = Config_reader()
    list_cnn_config = config_reader.get_list_src_name()
    print(list_cnn_config)
