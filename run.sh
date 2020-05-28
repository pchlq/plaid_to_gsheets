export PLAID_CLIENT_ID=''
export PLAID_SECRET=''
export PLAID_PUBLIC_KEY='3e65bdf376d84ab59a143ea56082e7'
export PLAID_PRODUCTS='transactions'
export PLAID_COUNTRY_CODES='US'
export PLAID_ENV='sandbox'
export PLAID_OAUTH_REDIRECT_URI='user_good'
export PLAID_OAUTH_NONCE='pass_good'
export ACCESS_TOKEN='access-sandbox-484e7570-8ae0-429f-ad16-ddaccadf8361'
export PERIOD_DAYS=100

python get_trans_plaid.py


export CREDENTIALS_FILE='creds.json'
export gsheetId='1Wc5jZU6KsciYPPgoF1B5MT2dCLkig050LB87BvmH6Hg'
export API_SERVICE_NAME='sheets'
export API_VERSION='v4'
export SCOPES="['https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive']"

python df_to_sheet.py