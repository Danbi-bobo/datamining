import sys
import os
from dotenv import load_dotenv

load_dotenv()

CDP_PATH = os.getenv("CDP_PATH")
if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

from pos_orders import main
from cdp.domain.utils.log_helper import setup_logger
import datetime

if __name__ == '__main__':
    setup_logger(__file__)

    end_time = int(datetime.datetime.now().timestamp())
    start_time = end_time - 3 * 24 * 60 * 60 - 5 * 60

    main(start_time=start_time, end_time=end_time)