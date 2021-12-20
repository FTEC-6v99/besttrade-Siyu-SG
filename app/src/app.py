from flask import Flask
from app.src.api.blueprints.investorbp import investorbp
from app.src.api.blueprints.accountbp import accountbp
from app.src.api.blueprints.portfoliobp import portfoliobp
from app.src.api.blueprints.uibp import uibp

app = Flask(__name__)
#register all endpoints
app.register_blueprint(investorbp)
app.register_blueprint(accountbp)
app.register_blueprint(portfoliobp)
app.register_blueprint(uibp)


# Test 
# @app.route('/')
# def default():
#     return  "Hello world!"




if __name__ == '__main__':
    app.run(port = 8080, debug = True)
    
