import arrow
import csv

from flusher import connect, daiquiri


# TODO: unit tests!!!
# TODO: inline documentation!!!
# TODO: save csv files in a specific directory

log = daiquiri.getLogger(__name__)


def now_str():
    return arrow.utcnow().format('YYYYMMDD_HHmmss')


def only_rangeletters(s):
    return ''.join(c.upper() for c in s if c.isalpha())


def colnumber(letters):
    return sum((ord(c)-64)*(25**(i)) for i,c in enumerate(letters))


def numcolumns_from_range(cellrange):
    start_letter, end_letter = map(only_rangeletters, cellrange.split(":"))
    return 1 + colnumber(end_letter) - colnumber(start_letter)


def numrows(worksheet):
    return len(worksheet.get_all_values())


def to_csv(document, sheet='', cellrange=''):
    gc = connect()

    # TODO: make it possible to specify the document by name, url or id
    sh = gc.open(document)
    # TODO: make it possible to specify the sheet by number or name
    wks = sh.worksheet(sheet) if sheet else sh.sheet1

    # TODO: when all above done, split this into 3+ functions
    #    - identify document/sheet
    #    - identify range
    #    - save to csv

    if cellrange:
        if not cellrange[-1].isdigit():
            cellrange += str(numrows(wks))

        raw_data = [c.value for c in wks.range(cellrange)]
        numbercolumns = numcolumns_from_range(cellrange)

    else:
        list_lists = wks.get_all_values()
        numbercolumns = len(list_lists[0])
        raw_data = [cell for row in list_lists for cell in row]
        cellrange = 'all'

    output_filename = '.'.join([document, sheet, cellrange, now_str(),'csv'])

    with open(output_filename, 'w') as fp:
        writer = csv.writer(fp, quoting=csv.QUOTE_NONNUMERIC)
        for rowcell in range(0, len(raw_data), numbercolumns):
            writer.writerow(raw_data[rowcell:rowcell+numbercolumns])

    return output_filename

