from flask import Flask

from controller.accidents_controller import accident_blueprint
from repository.csv_repository import drop_db
from repository.seed import seed

app = Flask(__name__)
app.register_blueprint(accident_blueprint, url_prefix='/api/accidents')

if __name__ == '__main__':
    drop_db()
    seed()
    #app.run(debug=True)