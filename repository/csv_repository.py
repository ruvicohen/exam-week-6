import csv
import os
from config.connect import accidents, areas, months, days, reasons, injuries, weeks
from utils.data_utils import parse_date, get_week_range, safe_int


def read_csv(csv_path):
    with open(csv_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            yield row


def init_data():

    with open(os.path.join(os.path.dirname(__file__), '..', 'assets', 'Traffic_Crashes_-_Crashes - 20k rows.csv'), mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            accident_date = parse_date(row['CRASH_DATE'])
            day_str = accident_date.strftime('%Y-%m-%d')
            month_str = accident_date.strftime('%Y-%m')
            accident_id = load_accident(row, accident_date)
            load_day(row, day_str)
            load_week(row, accident_date)
            load_month(row, month_str)
            load_area(row)
            load_injury(row, accident_id)
            load_reason(row, accident_id)










def load_accident(row, accident_date):
    accident = {
        'date': accident_date,
        'injuries': safe_int(row['INJURIES_TOTAL']),
        'fatal': safe_int(row['INJURIES_FATAL']),
        'reason': row['PRIM_CONTRIBUTORY_CAUSE'],  # Adding reason to the accident document
        'area': row['BEAT_OF_OCCURRENCE'],  # Adding area to the accident document
    }
    result = accidents.update_one(
        {'date': accident_date, 'injuries': row['INJURIES_TOTAL'], 'fatal': row['INJURIES_FATAL']},
        {'$set': accident},
        upsert=True
    )
    accident_id = result.upserted_id if result.upserted_id else accidents.find_one({'date': accident_date})['_id']
    return accident_id

def load_day(row, day_str):
    day_cache = {}
    area_name = row['BEAT_OF_OCCURRENCE']
    day_key = (day_str, area_name)
    if day_key not in day_cache:
        days.update_one(
            {'day': day_str, 'area_name': area_name},
            {'$set': {'total_accidents': 0}},  # Set total_accidents to 0 when inserting
            upsert=True
        )
        days.update_one(
            {'day': day_str, 'area_name': area_name},
            {'$inc': {'total_accidents': 1}}  # Increment only if already exists
        )
        day_cache[day_key] = True  # Mark as cached
    else:
        days.update_one(
            {'day': day_str, 'area_name': area_name},
            {'$inc': {'total_accidents': 1}}
        )


def load_week(row, accident_date):
    week_cache = {}
    area_name = row['BEAT_OF_OCCURRENCE']
    start_of_week, end_of_week = get_week_range(accident_date)
    week_str = f"{start_of_week.strftime('%Y-%m-%d')} to {end_of_week.strftime('%Y-%m-%d')}"

    week_key = (week_str, area_name)
    if week_key not in week_cache:
        weeks.update_one(
            {'week': week_str, 'area_name': area_name},
            {'$set': {'total_accidents': 0, 'start_date': start_of_week, 'end_date': end_of_week}},
            upsert=True
        )
        weeks.update_one(
            {'week': week_str, 'area_name': area_name},
            {'$inc': {'total_accidents': 1}}
        )
        week_cache[week_key] = True  # Mark as cached
    else:
        weeks.update_one(
            {'week': week_str, 'area_name': area_name},
            {'$inc': {'total_accidents': 1}}
        )


def load_month(row, month_str):
    month_cache = {}
    area_name = row['BEAT_OF_OCCURRENCE']

    month_key = (month_str, area_name)
    if month_key not in month_cache:
        months.update_one(
            {'month': month_str, 'area_name': area_name},
            {'$set': {'total_accidents': 0}},  # Set initial total_accidents to 0
            upsert=True
        )
        month_cache[month_key] = True  # Mark as cached
    else:
        months.update_one(
            {'month': month_str, 'area_name': area_name},
            {'$inc': {'total_accidents': 1}}
        )


def load_area(row):
    area_cache = {}
    area_name = row['BEAT_OF_OCCURRENCE']
    if area_name not in area_cache:
        area_cache[area_name] = areas.update_one(
            {'area_name': area_name},
            {'$inc': {'total_accidents': 1}},
            upsert=True
        )
    else:
        areas.update_one(
            {'area_name': area_name},
            {'$inc': {'total_accidents': 1}}
        )
        
def load_reason(row, accident_id):
    injury_key = 'cause_of_accident'
    area_name = row['BEAT_OF_OCCURRENCE']
    area_injury_key = f"{injury_key}_{area_name}"  # Adding area to injury key

    # Check if injury record exists for this area
    existing_injury = injuries.find_one({'cause_of_accident': injury_key, 'area_name': area_name})

    if existing_injury:
        fatal_accidents = existing_injury['fatal_accidents']
        injured_accidents = existing_injury['injured_accidents']
        fatal_total = existing_injury['fatal_total']
        injured_total = existing_injury['injured_total']

        # If there are fatalities
        if safe_int(row['INJURIES_FATAL']) > 0:
            fatal_accidents.append(accident_id)
            fatal_total += 1

        # If there are injuries
        elif safe_int(row['INJURIES_TOTAL']) > 0:
            injured_accidents.append(accident_id)
            injured_total += 1

        # Update injury record
        injuries.update_one(
            {'_id': existing_injury['_id']},
            {
                '$set': {
                    'fatal_accidents': fatal_accidents,
                    'fatal_total': fatal_total,
                    'injured_accidents': injured_accidents,
                    'injured_total': injured_total
                }
            }
        )
    else:
        # Create a new injury record if it doesn't exist
        injury_summary = {
            'fatal_accidents': [accident_id],  # First fatal accident
            'fatal_total': 1,  # Total fatalities
            'injured_accidents': [],  # No injuries yet
            'injured_total': 0,  # No injuries
            'area_name': area_name  # Area field
        }

        if safe_int(row['INJURIES_TOTAL']) > 0:
            injury_summary['injured_accidents'] = [accident_id]  # Add injury accident
            injury_summary['injured_total'] = 1  # Total injured

        injuries.insert_one(injury_summary)


def load_injury(row, accident_id):
    # Handling reasons by area
    area_name = row['BEAT_OF_OCCURRENCE']
    reason_key = row['PRIM_CONTRIBUTORY_CAUSE']
    reason_area_key = f"{reason_key}_{area_name}"  # Adding area to reason key

    reason_total = reasons.find_one({'reason': reason_key, 'area_name': area_name})
    if reason_total:
        # Update existing reason document by adding new accident
        reason_total['total_accidents'] += 1
        reason_total['accident_ids'].append(accident_id)
        reasons.update_one(
            {'reason': reason_key, 'area_name': area_name},
            {'$set': {'total_accidents': reason_total['total_accidents'],
                      'accident_ids': reason_total['accident_ids']}}
        )
    else:
        reasons.update_one(
            {'reason': reason_key, 'area_name': area_name},
            {'$set': {'total_accidents': 1, 'accident_ids': [accident_id], 'area_name': area_name}},
            upsert=True
        )


def drop_db():
    accidents.drop()
    areas.drop()
    days.drop()
    months.drop()
    injuries.drop()
    reasons.drop()


