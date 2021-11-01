# Database Access Object: file to interface with the database
# CRUD operations:
# C: Create
# R: Read
# U: Update
# D: Delete
import typing as t
from mysql.connector import connect, cursor
from mysql.connector.connection import MySQLConnection
import config
from app.src.domain.Investor import Investor
from app.src.domain.Account import Account
from app.src.domain.Portfolio import Portfolio

def get_cnx() -> MySQLConnection:
    return connect(**config.dbparams)

'''
    Investor DAO functions
'''

def get_all_investor() -> list[Investor]:
    '''
        Get list of all investors [R]
    '''
    investors: list[Investor] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from investor'
    cursor.execute(sql)
    results: list[dict] = cursor.fetchall()
    for row in results:
        investors.append(Investor(row['name'], row['status'], row['id']))
    db_cnx.close()
    return investors

def get_investor_by_id(id: int) -> t.Optional[Investor]:
    '''
        Returns an investor object given an investor ID [R]
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from investor where id = %s'
    cursor.execute(sql, (id,))
    if cursor.rowcount == 0:
        return None
    else:
        row = cursor.fetchone()
        investor = Investor(row['name'], row['status'], row['id'])
        return investor 

def get_investors_by_name(name: str) -> list[Investor]:
    '''
        Return a list of investors for a given name [R]
    '''
    investors: list[Investor] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from investor where name = %s'
    cursor.execute(sql, (name,))
    if cursor.rowcount == 0:
        investors = []
    else:
        rows = cursor.fetchall()
        for row in rows:
            investors.append(Investor(row['name'], row['status'], row['id']))
    db_cnx.close()
    return investors


def create_investor(investor: Investor) -> None:
    '''
        Create a new investor in the db given an investor object [C]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'insert into investor (name, status) values (%s, %s)'
    cursor.execute(sql, (investor.name, investor.status))
    db_cnx.commit()
    db_cnx.close()

def delete_investor(id: int):
    '''
        Delete an investor given an id [D]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'delete from investor where id = %s'
    cursor.execute(sql, (id,))
    db_cnx.commit() # inserts, updates, and deletes
    db_cnx.close()

def update_investor_name(id: int, name: str) -> None:
    '''
        Updates the investor name [U]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'update investor set name = %s where id = %s'
    cursor.execute(sql, (id, name))
    db_cnx.commit()
    db_cnx.close()

def update_investor_status(id: int, status: str) -> None:
    '''
        Update the inestor status [U]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'update investor set status = %s where id = %s'
    cursor.execute(sql, (id, status))
    db_cnx.commit()
    db_cnx.close()

'''
    Account DAO functions
'''
def get_all_accounts() -> list[Account]:
    '''
        Get a list of all accounts [R]
    '''
    accounts: list[Account] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from account'
    cursor.execute(sql)
    results: list[dict] = cursor.fetchall()
    for row in results:
        accounts.append(Account(row['account_number'], row['balance'], row['investor_id']))
    db_cnx.close()
    return accounts

def get_account_by_id(account_number: int) -> Account: 
    '''
        Get accounts given account number [R]
    '''    
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from account where account_number = %s'
    cursor.execute(sql, (account_number,))
    if cursor.rowcount == 0:
        return None
    else:
        row = cursor.fetchone()
        account = Account(row['account_number'], row['balance'], row['investor_id'])
    db_cnx.close()
    return account

def get_accounts_by_investor_id(investor_id: int) -> list[Account]:
    '''
        Get accounts given investor id [R]
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql:str = 'select * from account where investor_id = %s'
    cursor.execute(sql,(investor_id,)) # tuple  
    rows = cursor.fetchall()
    if len(rows) == 0: 
        return []
    accounts = []
    for row in rows:
        accounts.append(Account(row['account_number'], row['balance'], row['investor_id']))
    db_cnx.close()
    return accounts
    
def delete_account(account_number: int) -> None:
    '''
        delete accounts given account number 
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'delete from account where account_number = %s'
    cursor.execute(sql, (account_number,))
    db_cnx.commit() # inserts, updates, and deletes
    db_cnx.close()

def update_acct_balance(account_number: int, balance: float) -> None:
    '''
        Update account balance given account number
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'update account set balance = %s where account_number = %s'
    cursor.execute(sql, (account_number, balance))
    db_cnx.commit()
    db_cnx.close()

def create_account(account: Account) -> None:
    '''
        Create new account and add balance and account number[c]
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'insert into account (balance, account_number) values (%s, %s)'
    cursor.execute(sql, (account.balance, account.account_number))
    db_cnx.commit()
    db_cnx.close()

'''
    Portfolio DAO functions
'''
def get_all_portfolios() -> list[Portfolio]:
    '''
        Get a list of portfolio[R]
    '''
    portfolios: list[Portfolio] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from portfolio'
    cursor.execute(sql)
    results: list[dict] = cursor.fetchall()
    for row in results:
        portfolios.append(Portfolio( row['quantity'], row['ticker'], row['purchase_price'], row['account_number']))
    db_cnx.close()
    return portfolios
    

def get_portfolios_by_acct_id(account_number: int) -> list[Portfolio]:
    '''
        Get portfolios given account number/ account id
    '''
    portfolios: list[Portfolio] = []
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from portfolio where account_number = %s'
    cursor.execute(sql, (account_number,))
    if cursor.rowcount == 0:
        portfolios = []
    else:
        rows = cursor.fetchall()
        for row in rows:
            portfolios.append(Portfolio(row['quantity'], row['ticker'], row['purchase_price'],row['account_number']))
        db_cnx.close()
    return portfolios

def get_portfolios_by_investor_id(investor_id: int) -> list[Portfolio]:
    '''
        Get portfolios  given investor id
    '''
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True)
    sql:str = 'select * from portfolio left join account on account.account_number = portfolio.account_number where investor_id = %s;'
    cursor.execute(sql,(investor_id,)) 
    rows = cursor.fetchall()
    if len(rows) == 0: 
        return []
    portfolios = []
    for row in rows:
        portfolios.append(Portfolio(row['account_number'], row['ticker'], row['quantity'],row['purchase_price']))
    db_cnx.close()
    return portfolios
    
def delete_portfolio(account_number: int, ticker:str) -> None:
    '''
        Delete portfolio given by account number
    '''
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    sql = 'delete from portfolio where account_number = %s and ticker = %s'
    cursor.execute(sql, (account_number, ticker))
    db_cnx.commit() # inserts, updates, and deletes
    db_cnx.close()
    
def buy_stock(ticker: str, price: float, quantity: int,account_number: int) -> None:
    ''' 
        portfolio - p
        account - a
        for an investor acct, two cases:
            buy the same stocks(update: quantity(p), balance(a))
        OR buy different kinds of stocks- insert new records: ticker(p), purchase_price(p), quantity(p), and update balance(a)      
        
    '''
    #get current balance
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    sql: str = 'select * from portfolio where account_number= %s and ticker = %s;'
    cursor.execute(sql)
    result_bal = cursor.fetchone()
    current_balance = result_bal[2]
    db_cnx.close()

    #calculate and update account balance
    db_cnx: MySQLConnection = get_cnx()
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    balance_updated = current_balance - price * quantity
    if balance_updated <= 0:
        print('No balance left.')
        return None
    cursor.execute('update account set balance = %s where account_number = %s;', balance_updated, account_number)
    db_cnx.commit()
    db_cnx.close()

    #insert into new portfolio records
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    cursor.execute('select * from portfolio where account_number = %s and ticker = %s;',account_number,ticker)
    row = cursor.fetchone()
    if row == 0:
        db_cnx = get_cnx()
        cursor = db_cnx.cursor()
        cursor.execute('insert into portfolio (account_number, ticker, quantity, purchase_price) values (%s, "%s", %s, %s)')        
        db_cnx.commit()
        db_cnx.close()
    else: #update stock quantity 
        quantity_increment = row[2]
        quantity_updated = quantity + quantity_increment
        db_cnx = get_cnx()
        cursor = db_cnx.cursor()
        cursor.execute('update portfolio set quantity= %s where account_number = %s and ticker = %s;', quantity_updated,account_number,ticker)
        db_cnx.commit()
        db_cnx.close()

def sell_stock(ticker: str, quantity: int, sale_price: float, account_number:int) -> None:
    # 1. update quantity in portfolio table
    # 2. update the account balance:
    # Example: 10 APPL shares at $1/share with account balance $100, here purchase_price = 1
    # event: sale of 2 shares for $2/share
    # output: 8 APPLE shares at $1/share with account balance = 100 + 2 * (12 - 10) = $104
    
    #get stock quantity from portfolio  
    db_cnx: MySQLConnection = get_cnx()
    cursor = db_cnx.cursor(dictionary=True) # always pass dictionary = True
    cursor.execute('select * from portfolio where account_number = %s and ticker = %s;', account_number, ticker)
    row = cursor.fetchone()
    if row == 0:
        return None
    else:
        quantity_current = row['quantity']
    db_cnx.close()
    
    #get current balance and calculate balance affected
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    cursor.execute('select * from account where account_number =%s;', account_number) 
    row_bal = cursor.fetchone()
    current_balance = row_bal[2]
    balance_new = current_balance + quantity * sale_price
    db_cnx.close()
       
    #update portfolio table: new quantity after selling stocks(given account number and ticker)
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    quantity_new = quantity_current - quantity 
    cursor.execute('update portfolio set quantity= %s where account_number = %s and ticker = %s;', (quantity_new,account_number,ticker))
    db_cnx.commit()

    #update account table: new balance (given account number)
    db_cnx = get_cnx()
    cursor = db_cnx.cursor()
    cursor.execute('update account set balance = %s where account_number = %s;',balance_new,account_number)
    db_cnx.commit()
    db_cnx.close()

