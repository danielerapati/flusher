from time import sleep

from arrow import utcnow
from gspread.exceptions import *
from toolz import merge

from flusher import gc, daiquiri
from flusher.export import to_csv
from flusher.utils import instrumented


# TODO: logs sheet (latest is higher)
# TODO: refresh on a schedule
# TODO: create own control sheet if it does not exists (and share with who?)
# TODO: s3 destinations with boto and s3fs
# TODO: parallelism with multiprocessing
# TODO: inline documentation
# TODO: better instructions on sheet

MANAGER_DOCUMENT = 'Flush Control'
MANAGER_JOBS_WORKSHEET = 'Jobs Manager'

log = daiquiri.getLogger(__name__)


@instrumented(log.debug)
def read_control_sheet():
    return [merge(spec, {'row': rnumber+2}) # zero-based + 1 row of header
                for rnumber,spec in enumerate(jobs_sheet.get_all_records())
                if spec['Document']]


@instrumented(log.info)
def run_export(document, sheet, cellrange):
    try:
        return (to_csv(document, sheet, cellrange), None)
    except Exception as e:
        return (None, e)


@instrumented(log.debug)
def available_sheets(document):
    return [s.title for s in gc.open(document).worksheets()]


def translate_error(e, args):
    if type(e) == SpreadsheetNotFound:
        return """Unable to find document: {doc}.
You might need to share it with: {email}.
Otherwise check for typos.""".format(doc=args['document'], email=gc.auth.service_account_email)
    if type(e) == WorksheetNotFound:
        candidates = ', '.join(available_sheets(args['document']))
        return """Unable to find worksheet: {sheet}.
Check for typos.
Worksheets found: {candidates}.""".format(sheet=args['sheet'], candidates=candidates)
    else:
        return e


@instrumented(log.debug)
def update_running(job_spec):
    row = job_spec['row']

    refresh_now = jobs_sheet.cell(row, 4)
    refresh_now.value = ''
    state = jobs_sheet.cell(row, 7)
    state.value = 'Running'

    jobs_sheet.update_cells([refresh_now, state])


@instrumented(log.debug)
def update_success(job_spec, result):
    row = job_spec['row']

    refresh_now = jobs_sheet.cell(row, 4)
    refresh_now.value = ''
    last_success = jobs_sheet.cell(row, 6)
    last_success.value = utcnow().isoformat()
    state = jobs_sheet.cell(row, 7)
    state.value = 'Success'
    last_result = jobs_sheet.cell(row, 8)
    last_result.value = result

    jobs_sheet.update_cells([refresh_now, last_success, state, last_result])


@instrumented(log.debug)
def update_failure(job_spec, message):
    row = job_spec['row']

    refresh_now = jobs_sheet.cell(row, 4)
    refresh_now.value = ''
    state = jobs_sheet.cell(row, 7)
    state.value = 'Failure'
    last_result = jobs_sheet.cell(row, 8)
    last_result.value = message

    jobs_sheet.update_cells([refresh_now, state, last_result])


def should_run(job):
    return (not job['State'] == 'Running') \
        and job['Refresh Now']


@instrumented(log.info)
def run():
    while(True):
        sleep(1)
        registered_jobs = read_control_sheet()

        for job in registered_jobs:
            if should_run(job):
                update_running(job)

                args = {
                        'document': job['Document'],
                        'sheet': job['Sheet'],
                        'cellrange': job['Range']
                       }
                r, e = run_export(**args)
                if e:
                    update_failure(job,translate_error(e, args))
                else:
                    update_success(job, r)


control_doc = gc.open(MANAGER_DOCUMENT)
jobs_sheet = control_doc.worksheet(MANAGER_JOBS_WORKSHEET) if MANAGER_JOBS_WORKSHEET else control_doc.sheet1

