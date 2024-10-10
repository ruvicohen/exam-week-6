from datetime import datetime

from bson import ObjectId

from config.connect import accidents, areas, days, weeks, months, reasons


def get_sum_accidents_by_area(area: str):
    accidents_by_area = areas.find({'area_name': area}).to_list()
    return accidents_by_area

def get_sum_accidents_by_area_and_day(area: str, day: str):
    accidents_by_area_and_day = days.find({'area_name': area, 'day': day}).to_list()
    return accidents_by_area_and_day


def get_sum_accidents_by_area_and_start_date_for_week(area: str, start_date_str: str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    accidents_by_area_and_start_date = weeks.find({
        'area_name': area,
        'start_date': start_date
    }).to_list()
    return accidents_by_area_and_start_date

def get_sum_accidents_by_area_and_month(area: str, month: str):
    accidents_by_area_and_month = months.find({'area_name': area, 'month': month}).to_list()
    return accidents_by_area_and_month


def get_accidents_and_reason_by_area(area: str):
    reason_data = reasons.find_one({'area_name': area})

    if reason_data:
        accident_ids = reason_data.get('accident_ids', [])

        if accident_ids:
            accident_details = accidents.find({
                '_id': {'$in': [ObjectId(accident_id) for accident_id in accident_ids]}
            })

            return {
                'total_accidents': reason_data.get('total_accidents', 0),
                'accidents': list(accident_details)
            }

    return {'total_accidents': 0, 'accidents': []}

def get_accidents_and_stats_injury_by_area(area: str):
    injury_data = reasons.find_one({'area_name': area})

    if injury_data:
        accident_ids = injury_data.get('accident_ids', [])

        if accident_ids:
            accident_details = accidents.find({
                '_id': {'$in': [ObjectId(accident_id) for accident_id in accident_ids]}
            })

            return {
                'total_accidents': injury_data.get('total_accidents', 0),
                'accidents': list(accident_details)
            }

    return {'total_accidents': 0, 'accidents': []}

print(get_sum_accidents_by_area("1235"))
print(get_sum_accidents_by_area_and_month('1235', "2023-09"))
print(get_sum_accidents_by_area_and_day('1652', "2023-02-06"))
print(get_sum_accidents_by_area_and_start_date_for_week('1652', "2023-02-06"))
print(get_accidents_and_stats_injury_by_area('1652'))
print(get_accidents_and_reason_by_area('1652'))