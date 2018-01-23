import daiquiri
import gspread
import logging

from oauth2client.service_account import ServiceAccountCredentials


DEFAULT_LOG_FORMAT = daiquiri.formatter.DEFAULT_FORMAT.replace('%(name)s', '%(name)s.%(funcName)s')
daiquiri.setup(level=logging.INFO, outputs=(
    daiquiri.output.Stream(formatter=daiquiri.formatter.ColorFormatter(
        fmt=DEFAULT_LOG_FORMAT)),
    ))

GOOGLE_SERVICE_ACCOUNT_KEY_FILE = 'service-account-key.json'
scope = ['https://spreadsheets.google.com/feeds']


def connect():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SERVICE_ACCOUNT_KEY_FILE, scope)
    return gspread.authorize(credentials)
