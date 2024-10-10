from pymongo import MongoClient


client = MongoClient('mongodb://localhost:27017')

accidents_data_db = client['accidents_data']

accidents = accidents_data_db['accidents']
areas = accidents_data_db['areas']
days = accidents_data_db['days']
weeks = accidents_data_db['weeks']
months = accidents_data_db['months']
reasons = accidents_data_db['reasons']
injuries = accidents_data_db['injuries']
