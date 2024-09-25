import logging
from datetime import datetime
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# create logger
logger = logging.getLogger('compare_table_anmv1')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

fh = logging.FileHandler(r'compare_log_{date}.log'.format(date=datetime.today().strftime('%Y%m%d')))

# add formatter to ch
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
logger.addHandler(fh)
