from flask import Flask, jsonify, request, Blueprint
from repository.accidents_repository import get_sum_accidents_by_area, get_sum_accidents_by_area_and_day, \
    get_sum_accidents_by_area_and_start_date_for_week, get_sum_accidents_by_area_and_month, \
    get_accidents_and_stats_injury_by_area, get_accidents_and_reason_by_area
from utils.data_utils import parse_date, parse_json

accident_blueprint = Blueprint('accidents', __name__)

# Route to get accidents by area
@accident_blueprint.route('/area/<area>', methods=['GET'])
def get_accidents_by_area(area):
    result = get_sum_accidents_by_area(area)
    return parse_json(result), 200

# Route to get accidents by area and day
@accident_blueprint.route('/area/<area>/day/<day>', methods=['GET'])
def get_accidents_by_area_and_day(area, day):
    result = get_sum_accidents_by_area_and_day(area, day)
    return parse_json(result)

# Route to get accidents by area and start date (week)
@accident_blueprint.route('/area/<area>/week/<start_date>', methods=['GET'])
def get_accidents_by_area_and_start_date_for_week(area, start_date):
    result = get_sum_accidents_by_area_and_start_date_for_week(area, start_date)
    return parse_json(result)

# Route to get accidents by area and month
@accident_blueprint.route('/area/<area>/month/<month>', methods=['GET'])
def get_accidents_by_area_and_month(area, month):
    result = get_sum_accidents_by_area_and_month(area, month)
    return parse_json(result)

# Route to get accidents and reasons by area
@accident_blueprint.route('/area/<area>/reason', methods=['GET'])
def get_accidents_and_reason_by_area_c(area):
    result = get_accidents_and_reason_by_area(area)
    return parse_json(result)

# Route to get accidents and injury stats by area
@accident_blueprint.route('/area/<area>/injuries', methods=['GET'])
def get_accidents_and_stats_injury_by_area_c(area):
    result = get_accidents_and_stats_injury_by_area(area)
    return parse_json(result)

