import os
import re
import ast
import pandas as pd
import numpy as np
from typing import List, Union
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials


CREDENTIALS_FILE = os.getenv('CREDENTIALS_FILE')
gsheetId = os.getenv('gsheetId')
API_SERVICE_NAME = os.getenv('API_SERVICE_NAME')
API_VERSION = os.getenv('API_VERSION')
SCOPES = ast.literal_eval(os.environ.get("SCOPES"))

credentials = ServiceAccountCredentials.from_json_keyfile_name(
 CREDENTIALS_FILE, SCOPES
)

httpAuth = credentials.authorize(httplib2.Http())
service = apiclient.discovery.build(API_SERVICE_NAME, API_VERSION, http = httpAuth)

def export_data_to_sheet(df: pd.DataFrame, 
                         listName: str
                         ) -> None:

    response_date = service.spreadsheets().values().update(
        spreadsheetId=gsheetId,
        valueInputOption='RAW',
        range=f'{listName}!a1',
        body=dict(
            majorDimension='ROWS',
            values=df.T.reset_index().T.values.tolist())
    ).execute()


def add_sheet(title: str, 
              nrows: int,
              ncols: int
              ) -> None:

    results = service.spreadsheets().batchUpdate(
    spreadsheetId = gsheetId,
    body = 
    {
    "requests": [
        {
        "addSheet": {
            "properties": {
            "title": title,
            "gridProperties": {
                "rowCount": nrows,
                "columnCount": ncols
                }
            }
        }
        }
    ]
    }).execute()


def clear_sheet(sheetName: str) -> None:
    
    rangeAll = '{0}!A1:Z'.format( sheetName )
    resultClear = service.spreadsheets().values().clear(spreadsheetId=gsheetId, 
                                                        range=rangeAll,
                                                        body={} ).execute()

def get_sheets_properties() -> dict:
    spreadsheet = service.spreadsheets().get(spreadsheetId = gsheetId).execute()
    sheetList = spreadsheet.get('sheets')
    dct_sheets = {}
    for sheet in sheetList:
        dct_sheets[sheet['properties']['title']] = sheet['properties']['sheetId']
    
    return dct_sheets



def clean_categories1(row: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_\s,]+', '', row).split(', ')[0]

def clean_categories2(row: Union[str, list]
                     ) -> Union[str, list]:
    
    res = re.sub(r'[^a-zA-Z0-9_\s,]+', '', row).split(', ')[1:]
    category2 = res if res else 'null'
    return category2


def clean_data_trans() -> pd.DataFrame:
    
    df = pd.read_csv("raw_tansactions100_2705.csv", 
                    parse_dates=['date'], 
                    usecols=['amount', 'category', 'date'])

    split_categories = lambda x: x.split(',')
    df['category 1'] = df.category.apply(clean_categories1)
    df['category 2'] = df.category.apply(clean_categories2)
    df.drop('category', axis=1, inplace=True)
    df = df.explode('category 2')
    
    df['Income/Expense'] = df.amount.map(lambda x: 'Income' if x>0 else 'Expense')
    df['date'] = df.date.map(lambda x: x.strftime('%b %Y'))
    df.replace(np.nan, '', inplace=True)

    global month_count
    month_count = df.date.nunique()

    global dct_cols
    dct_cols = dict(zip(df.columns.tolist(), range(len(df.columns))))
    return df

def make_pivotTbl(source_sheet_id: int, 
                  target_sheet_id: int,
                  end_row: int,
                  end_col: int) -> None:

    requests = []
    requests.append({
        'updateCells': {
            'rows': {
                'values': [
                    {
                        'pivotTable': {
                            'source': {
                                'sheetId': source_sheet_id,
                                'startRowIndex': 0,
                                'startColumnIndex': 0,
                                'endRowIndex': end_row,
                                'endColumnIndex': end_col
                            },
                            'rows': [
                                {
                                    "sourceColumnOffset": dct_cols['Income/Expense'],
                                    "showTotals": True,
                                    "sortOrder": "DESCENDING"
                                },

                                {
                                    "sourceColumnOffset": dct_cols['category 1'],
                                    "showTotals": True,
                                    "sortOrder": "ASCENDING"
                                },

                                {
                                    'sourceColumnOffset': dct_cols['category 2'],
                                    'showTotals': True,
                                    'sortOrder': 'ASCENDING',

                                }

                            ],
                            'columns': [
                                {
                                    'sourceColumnOffset': dct_cols['date'],
                                    'sortOrder': 'ASCENDING',
                                    'showTotals': True,

                                }
                            ],
                            'values': [
                                {
                                    'summarizeFunction': 'SUM',
                                    'sourceColumnOffset': dct_cols['amount']
                                }

                            ],
                            'valueLayout': 'HORIZONTAL'
                        }
                    }
                ]
            },
            'start': {
                'sheetId': target_sheet_id,
                'rowIndex': 0,
                'columnIndex': 0
            },
            'fields': 'pivotTable'
        }
    })
    
    requests.append( # setting number formart
        {
            "repeatCell": {
                "range": {
                    'sheetId': target_sheet_id,
                    'startRowIndex': 2,
                    'startColumnIndex': 3,
                    'endRowIndex': 26,
                    'endColumnIndex': 8
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "CURRENCY",
                            "pattern": "\"\"#,###.##"
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        }
    )

    requests.append({ # setting autoresize columns
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": target_sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 1,  # set the column index
                    "endIndex": 4  # set the column index
                }
            }
        }
    )

    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": target_sheet_id,
                    "startColumnIndex": 7,
                    "endColumnIndex": 8
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat.textFormat.bold"
            }
        }

    )


    body = {
        'requests': requests
    }
    response = service.spreadsheets().batchUpdate(spreadsheetId=gsheetId, body=body).execute()


if __name__ == '__main__':

    raw_data = pd.read_csv("raw_tansactions100_2705.csv"))
    raw_data.replace(np.nan, '', inplace=True)
    export_data_to_sheet(raw_data, rawName)

    df = clean_data_trans()
    nrows, ncols = df.shape
    add_sheet("cleaned_data", nrows, ncols)
    export_data_to_sheet(df, "cleaned_data")

    add_sheet("cashflow_statement", 50, 15)
    dct_sheets = get_sheets_properties()
    source_sheet_id = dct_sheets["cleaned_data"]
    target_sheet_id = dct_sheets["cashflow_statement"]
    clear_sheet("cashflow_statement")
    make_pivotTbl(source_sheet_id, target_sheet_id,
                  nrows, ncols)