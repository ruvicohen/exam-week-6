from config.connect import accidents_data_db
from repository.csv_repository import init_data


def seed():
    if accidents_data_db['accidents'].count_documents({}) == 0:
        init_data()