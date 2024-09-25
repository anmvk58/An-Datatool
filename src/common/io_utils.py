import sys, os
from src.common.default_var import DefaultVar
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class IOUtils:

    @staticmethod
    def get_absolute_path(relative_path: str) -> str:
        root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        return os.path.join(root_dir, relative_path)

    @staticmethod
    def get_extract_folder_by_partition(table_name, business_date):
        relative_path = [DefaultVar.EXTRACT_FOLDER, table_name, business_date]
        return IOUtils.get_absolute_path(os.path.join(*relative_path))

    @staticmethod
    def get_src_path(str):
        driver_path = str[0: str.find('an-datatool\src') + len('an-datatool\src')] + "\\connector\\drivers"
        list_driver = []
        for driver in os.listdir(driver_path):
            list_driver.append(driver_path + f"\\{driver}")
        return list_driver