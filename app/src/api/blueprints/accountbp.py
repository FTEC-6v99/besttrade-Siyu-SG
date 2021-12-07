import typing as t
import json
from flask import Blueprint
import app.src.db.dao as dao
from app.src.domain.Account import Account

accountbp = bp = Blueprint('account', __name__, url_prefix = '/account')


@bp.route('/get-all',methods = ['GET'])
def get_all_accounts():
    accounts: t.List[Account] = dao.get_all_accounts()
    if len(accounts) == 0:
        return json.dumps([])
    else:
        return json.dumps(accounts, default=lambda x: x.__dict__)



@bp.route('/get-account-by-account-number/<id>')
def get_account_by_account_number(account_number: int):
    account: Account = dao.get_account_by_account_number(id)
    if account is None:
        return json.dumps('')
    return json.dumps(account, default=lambda x: x.__dict__)


@bp.route('/get-accounts-by-investor-id/<investor_id>')
def get_accounts_by_investor_id(investor_id: int):
    accounts: t.List[Account] = dao.get_accounts_by_investor_id(investor_id)
    if len(accounts) == 0:
        return json.dumps([])
    else:
        return json.dumps(accounts, default=lambda x: x.__dict__)


@bp.route('/get-accounts-by-name/<name>')
def get_accounts_by_name(name):
    accounts: t.List[Account] = dao.get_accounts_by_name(name)
    if len(accounts) == 0:
        return json.dumps([])
    else:
        return json.dumps(accounts, default=lambda x: x.__dict__)

@bp.route('/create-account/<investor_id>/<balance>', methods=['POST'])
def create_account(investor_id, balance):
    dao.create_account(investor_id, balance)
    return '', 200


@bp.route('/update-acct-balance/<account_number>/<balance>', methods=['PUT'])
def update_acct_balance(account_number, balance):
    dao.update_acct_balance(account_number, balance)
    return '', 200


@bp.route('/delete-account/<investor_id>', methods=['DELETE'])
def delete_account(investor_id):
    dao.delete_account(investor_id)
    return '', 200
