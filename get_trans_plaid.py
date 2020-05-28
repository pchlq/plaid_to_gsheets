import os
import plaid
import datetime
import pandas as pd

PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
PLAID_SECRET = os.getenv('PLAID_SECRET')
PLAID_PUBLIC_KEY = os.getenv('PLAID_PUBLIC_KEY')
PLAID_ENV = os.getenv('PLAID_ENV')
PLAID_PRODUCTS = os.getenv('PLAID_PRODUCTS')
PLAID_COUNTRY_CODES = os.getenv('PLAID_COUNTRY_CODES')
PLAID_OAUTH_REDIRECT_URI = os.getenv('PLAID_OAUTH_REDIRECT_URI')
PLAID_OAUTH_NONCE = os.getenv('PLAID_OAUTH_NONCE')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
PERIOD_DAYS = int( os.environ.get('PERIOD_DAYS') )
TODAY = datetime.datetime.today().strftime("%d%m")

client = plaid.Client(client_id = PLAID_CLIENT_ID, secret=PLAID_SECRET,
                      public_key=PLAID_PUBLIC_KEY, environment=PLAID_ENV, api_version='2019-05-29')

start_date = '{:%Y-%m-%d}'.format(datetime.datetime.now() + datetime.timedelta(-PERIOD_DAYS))
end_date = '{:%Y-%m-%d}'.format(datetime.datetime.now())
transactions_response = client.Transactions.get(ACCESS_TOKEN, start_date, end_date)


if __name__=='__main__':
    
    ( pd.DataFrame.from_dict(transactions_response['transactions'])
        .to_csv(f'raw_tansactions{PERIOD_DAYS}_{TODAY}.csv', index=False) )