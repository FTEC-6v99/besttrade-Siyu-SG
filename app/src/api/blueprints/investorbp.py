import typing as t
import json
from flask import Blueprint
import app.src.db.dao as dao
from app.src.domain.Investor import Investor

investorbp = bp = Blueprint('investor', __name__, url_prefix = '/investor')

#worked
@bp.route('/get-all',methods = ['GET'])
def get_all_investor(): 
    try:
        investors:t.List[Investor] = dao.get_all_investor()
        if investors is None:
            return json.dumps([]), 200
        else:
            return json.dumps(investors, default=lambda x: x.__dict__), 200
    except Exception as e:
        return 'Error occurred' + str(e), 500

    
#worked
@bp.route('/get-investor-by-id/<id>', methods = ['GET'])
def get_investor_by_id(id:int) -> Investor:
    try:
        investors: Investor = dao.get_investor_by_id(id)
        if investors is None:
            return json.dumps([]),200
        else: 
            return json.dumps(investors, default = lambda x: x.__dict__)
    except Exception as e:
        return 'Oops, an error occured' + str(e), 500

#worked
@bp.route('/get-investor-by-name/<name>', methods = ['GET'])
def get_investor_by_name(name:str) -> t.List[Investor]:
    try:
        investors:t.List[Investor] = dao.get_investors_by_name(name)
        if investors is None:
            return json.dumps([]),200
        else:
            return json.dumps(investors, default = lambda x: x.__dict__),200
    except Exception as e:
        return 'Oops, an error occured' + str(e), 500

#worked
@bp.route('/create-new-investor/<name>/<status>', methods=['POST'])
def create_investor(name, status):
    investor: Investor = Investor(name, status)
    dao.create_investor(investor)
    return json.dumps(investor.__dict__)
    #return 'created', 200

#worked
@bp.route('/delete/<id>', methods = ['DELETE'])
def delete_investor(id):
    try:
        dao.delete_investor(id)
        return 'Deleted'
    except Exception as e:
        return  'Oops, an error occured:' + str(e), 500
#worked
@bp.route('/update-investor-name/<id>/<name>', methods = ['PUT'])
def update_investor_name(id,name):
    try:
        dao.update_investor_name(id,name)
        return 'Updated name'
    except Exception as e:
        return  'Oops, an error occured:' + str(e), 500
#worked
@bp.route('/update-investor-status/<id>/<status>', methods = ['PUT'])
def update_investor_status(id,status):
    try:
        dao.update_investor_status(id,status)
        return 'Updated status'
    except Exception as e:
        return  'Oops, an error occured:' + str(e), 500




