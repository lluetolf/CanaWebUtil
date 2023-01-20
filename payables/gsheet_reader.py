from __future__ import print_function

import json
from datetime import datetime
import os.path

from helper.category_mapper import categorize, KNOWN_MAPPINGS
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
# y2020 = '1q_JQJfS2kaRxyC4SkHjoXGkbhi_0CiEQ68R6hbqWPy4'
# y2021 = '1tLJot5UnZpqz0oKKJWk-J0sLVT-RBhTTOuSZbGgcN50'
y2022 = '1-p-f7lUvHLkcs4C-tKPZKUGrbjFg1b05Loj9jKxjnNI'

MONTHS = ['NOV']
# MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']


def connect():
    creds = None
    token_path = '../config/token.json'
    credential_path = '../config/credentials.json'
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credential_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
    return creds


def sanity_check_sheets(sheets: [{}]):
    if len(sheets) < 12:
        print("Incomplete number of sheets")
        return False

    diff = set(MONTHS).difference(set(map(lambda x: x['properties']['title'].upper(), sheets)))

    if len(diff) > 0:
        print("Some months are not covered: " + ", ".join(list(diff)))
        return False

    return True


def read_records(spreadsheet_id, credentials) -> []:
    try:
        service = build('sheets', 'v4', credentials=credentials)

        # Call the Sheets API
        sheet = service.spreadsheets()
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        if not sanity_check_sheets(sheets):
            exit(-1)

        values = []
        for m in MONTHS:
            result = sheet.values().get(spreadsheetId=spreadsheet_id, range=m).execute()
            values.extend(result.get('values', []))

        if not values:
            print('No data found.')
            return
        return values
    except HttpError as err:
        print(err)


def transform_entry(entry: []) -> {}:
    category, found = categorize(entry[2])
    if not found:
        category += "-UNMATCHED"

    fb_entry = {
        "category": f"{category}",
        "comment": "",
        "documentId": f"{entry[1]}",
        "fieldNames": list(),
        "lastUpdated": datetime.now(),
        "pricePerUnit": float(entry[3].replace('$', '').replace(',', '')),
        "quantity": 1,
        "subCategory": entry[2],
        "transactionDate": datetime.strptime(entry[0], "%d.%m.%Y")
    }
    return fb_entry


#
# Main
#
creds = connect()
all_records = []
for y in [y2022]:
    all_records.extend(read_records(y, creds))

with open("temp.json", "w") as f:
    json.dump(all_records, f)


# with open("temp.json", "r") as f:
#     all_records = json.load(f)

records = list(filter(lambda x: len(x) > 3, all_records))
records = list(filter(lambda x: x[0] != "Fetcha" and x[1] != "Folio", records))
records = list(filter(lambda x: x[2] != "Suma Total" and x[0] != "", records))


fb_entries = list(map(lambda x: transform_entry(x), records))

print(f"Entires: {len(fb_entries)}")


COLLECTION_NAME = "Payable"
PROJECT_ID = "dev-canaweb-firestore"
cred = credentials.Certificate('../config/firestore-key2.json')
app = firebase_admin.initialize_app(cred)
firestore_client = firestore.client()
for entry in fb_entries:
    doc_ref = firestore_client.collection(COLLECTION_NAME).document()
    doc = doc_ref.get()
    doc_ref.set(entry)
