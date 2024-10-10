import csv
import os

from pymongo import UpdateOne

from config.connect import accidents, areas, months, days, reasons, injuries, weeks
from utils.data_utils import parse_date, get_week_range, safe_int
import pandas as pd


def read_csv(csv_path):
    with open(csv_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            yield row


def init_data():
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'assets', 'Traffic_Crashes_-_Crashes.csv')
    df = pd.read_csv(csv_path)

    # Prepare bulk updates for all collections
    day_operations = []
    week_operations = []
    month_operations = []
    area_operations = []
    injury_operations = []
    reason_operations = []
    a = 0
    for index, row in df.iterrows():
        a += 1
        print(a)
        accident_date = parse_date(row['CRASH_DATE'])
        day_str = accident_date.strftime('%Y-%m-%d')
        month_str = accident_date.strftime('%Y-%m')
        accident_id = load_accident(row, accident_date)

        # Prepare operations for bulk updates
        day_operations.append(UpdateOne(
            {'day': day_str, 'area_name': row['BEAT_OF_OCCURRENCE']},
            {'$inc': {'total_accidents': 1}},
            upsert=True
        ))

        start_of_week, end_of_week = get_week_range(accident_date)
        week_operations.append(UpdateOne(
            {'area_name': row['BEAT_OF_OCCURRENCE']},
            {'$set': {'total_accidents': 0, 'start_date': start_of_week, 'end_date': end_of_week}},
            upsert=True
        ))

        month_operations.append(UpdateOne(
            {'month': month_str, 'area_name': row['BEAT_OF_OCCURRENCE']},
            {'$inc': {'total_accidents': 1}},
            upsert=True
        ))

        area_operations.append(UpdateOne(
            {'area_name': row['BEAT_OF_OCCURRENCE']},
            {'$inc': {'total_accidents': 1}},
            upsert=True
        ))

        injury_operations.append(create_injury_update(accident_id, row))  # Refactored injury update logic
        reason_operations.append(create_reason_update(accident_id, row))  # Refactored reason update logic

    # Perform all bulk updates
    days.bulk_write(day_operations)
    weeks.bulk_write(week_operations)
    months.bulk_write(month_operations)
    areas.bulk_write(area_operations)
    injuries.bulk_write(injury_operations)
    reasons.bulk_write(reason_operations)


def create_injury_update(accident_id, row):
    injury_key = 'cause_of_accident'
    area_name = row['BEAT_OF_OCCURRENCE']
    injury_summary = {
        'fatal_accidents': [],
        'fatal_total': 0,
        'injured_accidents': [],
        'injured_total': 0,
        'area_name': area_name
    }

    if safe_int(row['INJURIES_TOTAL']) > 0:
        injury_summary['injured_accidents'] = [accident_id]
        injury_summary['injured_total'] = 1
    if safe_int(row['INJURIES_FATAL']) > 0:
        injury_summary['fatal_accidents'] = [accident_id]
        injury_summary['fatal_total'] = 1

    return UpdateOne(
        {'cause_of_accident': injury_key, 'area_name': area_name},
        {'$set': injury_summary},
        upsert=True
    )

def load_accident(row, accident_date):
    accident = {
        'date': accident_date,
        'injuries': safe_int(row['INJURIES_TOTAL']),
        'fatal': safe_int(row['INJURIES_FATAL']),
        'reason': row['PRIM_CONTRIBUTORY_CAUSE'],
        'area': row['BEAT_OF_OCCURRENCE']
    }
    result = accidents.insert_one(accident )
    accident_id = result.inserted_id
    return accident_id


def create_reason_update(accident_id, row):
    area_name = row['BEAT_OF_OCCURRENCE']
    reason_key = row['PRIM_CONTRIBUTORY_CAUSE']

    reason_summary = {
        'reason': reason_key,
        'area_name': area_name,
        'accident_ids': [accident_id],
        'total_accidents': 1
    }

    return UpdateOne(
        {'reason': reason_key, 'area_name': area_name},
        {'$set': reason_summary},
        upsert=True
    )
def drop_db():
    accidents.drop()
    areas.drop()
    days.drop()
    months.drop()
    injuries.drop()
    reasons.drop()


