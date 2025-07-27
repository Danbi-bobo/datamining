import sys
import os
from dotenv import load_dotenv
load_dotenv()
CDP_PATH = os.getenv("CDP_PATH")

if CDP_PATH and CDP_PATH not in sys.path:
    sys.path.append(CDP_PATH)

import subprocess
import pandas as pd
from numpy import nan
from cdp.adapters.mariadb.mariadb_handler import MariaDBHandler
from cdp.domain.utils.log_helper import setup_logger
import logging
import re

def extract_job_from_command(command):
    match = re.search(r'entries/(.*)', command)
    return match.group(1) if match else None

def list_cronjobs_to_df(user=None):
    try:
        cmd = ['crontab', '-l']
        if user:
            cmd.extend(['-u', user])
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        lines = result.stdout.strip().split('\n')

        cron_list = []
        for line in lines:
            raw_line = line.strip()
            if not raw_line or raw_line.startswith('#'):
                continue

            if raw_line.startswith('@'):
                parts = raw_line.split(maxsplit=1)
                if len(parts) < 2:
                    continue
                cron_list.append({
                    'minute': nan,
                    'hour': nan,
                    'day': nan,
                    'month': nan,
                    'weekday': parts[0],
                    'command': parts[1],
                    'raw_line': raw_line
                })
                continue

            parts = raw_line.split()
            if len(parts) < 6:
                continue

            minute = parts[0]
            hour = parts[1]
            day_of_month = parts[2]
            month = parts[3]
            week_day = parts[4]
            command = ' '.join(parts[5:])
            job = extract_job_from_command(command)

            cron_list.append({
                'minute': minute,
                'hour': hour,
                'day': day_of_month,
                'month': month,
                'weekday': week_day,
                'job': job,
                'command': command,
                'raw_line': raw_line
            })

        return pd.DataFrame(cron_list)

    except subprocess.CalledProcessError as e:
        if e.returncode == 1:
            logging.info("Người dùng không có cronjob nào.")
        else:
            logging.info("Lỗi khi đọc crontab:", e)
        return pd.DataFrame()

if __name__ == "__main__":
    setup_logger(__file__)
    df = list_cronjobs_to_df()
    df.replace(nan, None, inplace=True)
    if not df.empty:
        MariaDBHandler().insert_and_update_from_df(
            database="lollibooks_dev",
            table="cronjobs", 
            df=df, 
            db_type="golden",
            overwrite_table=True,
            log=True,
            unique_columns=[]
        )
