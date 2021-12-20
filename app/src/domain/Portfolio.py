class Portfolio():
    # create a class that mimics the database table: portfolio
    def __init__(self, account_id, ticker, quantity, purchase_price):
        self.account_id = account_id
        self.ticker = ticker
        self.quantity = quantity
        self.purchase_price = purchase_price