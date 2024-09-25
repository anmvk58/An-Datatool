from src.extract_parquet.DB2_extractor import DB2Extractor
from src.extract_parquet.MSSQL_extractor import MssqlExtractor
from src.extract_parquet.Oracle_extractor import OracleExtractor

class Extractor_factory:
    @staticmethod
    def create_extractor(engine_type, query, src_name):
        if(engine_type == 'DB2'):
            return DB2Extractor(query, src_name)
        elif(engine_type == 'ORACLE'):
            return OracleExtractor(query, src_name)
        elif(engine_type == 'MSSQL'):
            return MssqlExtractor(query, src_name)
        else:
            # logger.error("Not founded: engine %s" % engine)
            raise NotImplementedError("Not founded: engine %s" % engine_type)
