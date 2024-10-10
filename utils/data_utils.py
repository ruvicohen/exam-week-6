import json
from datetime import datetime, timedelta


from datetime import datetime

from datetime import datetime

from bson import json_util


def parse_date(date_str: str):
    has_seconds = len(date_str.split(' ')) > 2
    date_format = '%m/%d/%Y %H:%M:%S %p' if has_seconds else '%m/%d/%Y %H:%M'
    # המרת התאריך ל- datetime כולל שעה דיפולטיבית
    date_obj = datetime.strptime(date_str, date_format)
    return date_obj.replace(hour=0, minute=0, second=0)  # שים שעה דיפולטיבית


def get_week_range(date):
    start_of_week = date - timedelta(days=date.weekday())  # Monday
    end_of_week = start_of_week + timedelta(days=6)        # Sunday
    return start_of_week, end_of_week

def safe_int(value, default=0):
    try:
        return int(value) if value.strip() else default
    except ValueError:
        return default

def parse_json(data):
    return json.loads(json_util.dumps(data))