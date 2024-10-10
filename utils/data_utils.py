import json
from datetime import datetime, timedelta


from datetime import datetime

from datetime import datetime

import pandas as pd
from bson import json_util


def parse_date(date_str: str):
    has_seconds = len(date_str.split(' ')) > 2
    date_format = '%m/%d/%Y %H:%M:%S %p' if has_seconds else '%m/%d/%Y %H:%M'
    date_obj = datetime.strptime(date_str, date_format)
    return date_obj.replace(hour=0, minute=0, second=0)


def get_week_range(date):
    start_of_week = date - timedelta(days=date.weekday())  # Monday
    end_of_week = start_of_week + timedelta(days=6)        # Sunday
    return start_of_week, end_of_week

def safe_int(value, default=0):
    # If value is a float, handle it accordingly
    if isinstance(value, float):
        return int(value) if not pd.isna(value) else default

    # If value is a string, strip and convert to int
    return int(value.strip()) if isinstance(value, str) and value.strip() else default

def parse_json(data):
    return json.loads(json_util.dumps(data))