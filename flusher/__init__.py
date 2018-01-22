import daiquiri
import gspread
import logging

from oauth2client.service_account import ServiceAccountCredentials


DEFAULT_LOG_FORMAT = daiquiri.formatter.DEFAULT_FORMAT.replace('%(name)s', '%(name)s.%(funcName)s')
daiquiri.setup(level=logging.INFO, outputs=(
    daiquiri.output.Stream(formatter=daiquiri.formatter.ColorFormatter(
        fmt=DEFAULT_LOG_FORMAT)),
    ))


scope = ['https://spreadsheets.google.com/feeds']

credentials = ServiceAccountCredentials.from_json_keyfile_name('service-account-key.json', scope)

# TODO: reauthorize when connection fails ... token expires after a few hours
def connect():
    return gspread.authorize(credentials)
