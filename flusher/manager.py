from os import remove
from threading import Thread
from time import sleep

from arrow import utcnow
from gspread.exceptions import *
from toolz import merge

from flusher import connect, daiquiri
from flusher.export import to_csv
from flusher.load.bigquery import load as bqload
from flusher.refresh_interval import is_scheduled
from flusher.utils import instrumented


# TODO: create own control sheet if it does not exists (and share with who?)
# TODO: better instructions on sheet
# TODO: s3 destinations with boto and s3fs
# TODO: discover jobs manager worksheet structure rather than hard-coding it
# TODO: better errors, more information on runs (?better log messages?)
# TODO (ideally): redshift destination with facility for table creation ...
# TODO: parallelism with multiprocessing
# TODO: inline documentation
# TODO: Job class

MANAGER_DOCUMENT = 'Flush Control'
MANAGER_JOBS_WORKSHEET = 'Jobs Manager'
MANAGER_LOGS_WORKSHEET = 'Log'

log = daiquiri.getLogger(__name__)

gc = connect()


@instrumented(log.debug)
def read_control_sheet():
    return [merge(spec, {'row': rnumber+2}) # zero-based + 1 row of header
            for rnumber, spec in enumerate(jobs_sheet.get_all_records())
            if spec['Document']]


@instrumented(log.info)
def run_export(document, sheet, cellrange):
    try:
        return (to_csv(document, sheet, cellrange), None)
    except Exception as e:
        return (None, e)


@instrumented(log.info)
def run_load(source_file, job_spec):
    try:
        target_system = job_spec['Target System'].lower().replace(' ', '')
        destination = job_spec['Destination'].lower().replace(' ', '')
        incremental = job_spec.get('Incremental')
        if target_system == 'bigquery':
            return (bqload(source_file, destination, incremental), None)
        else:
            raise NotImplementedError('Not Implemented: ' + target_system)

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

    refresh_now = jobs_sheet.cell(row, 7)
    refresh_now.value = ''
    state = jobs_sheet.cell(row, 10)
    state.value = 'Running'

    jobs_sheet.update_cells([refresh_now, state])

    return utcnow().isoformat()


@instrumented(log.debug)
def update_success(job_spec, result):
    row = job_spec['row']

    refresh_now = jobs_sheet.cell(row, 7)
    refresh_now.value = ''
    last_success = jobs_sheet.cell(row, 9)
    last_success.value = utcnow().isoformat()
    state = jobs_sheet.cell(row, 10)
    state.value = 'Success'
    last_result = jobs_sheet.cell(row, 11)
    last_result.value = result

    jobs_sheet.update_cells([refresh_now, last_success, state, last_result])

    return utcnow().isoformat()


@instrumented(log.debug)
def update_failure(job_spec, message):
    row = job_spec['row']

    refresh_now = jobs_sheet.cell(row, 7)
    refresh_now.value = ''
    refresh_interval = jobs_sheet.cell(row, 8)
    refresh_interval.value = ''
    state = jobs_sheet.cell(row, 10)
    state.value = 'Failure'
    last_result = jobs_sheet.cell(row, 11)
    last_result.value = message

    jobs_sheet.update_cells([refresh_now, refresh_interval, state, last_result])

    return utcnow().isoformat()


@instrumented(log.debug)
def update_invalid_schedule(job_spec, message):
    row = job_spec['row']

    refresh_interval = jobs_sheet.cell(row, 8)
    refresh_interval.value = ''
    state = jobs_sheet.cell(row, 10)
    state.value = 'Failure'
    last_result = jobs_sheet.cell(row, 11)
    last_result.value = message

    jobs_sheet.update_cells([refresh_interval, state, last_result])


def add_log_line(job_args, result, error, start, end):
    logs_sheet = control_doc.worksheet(MANAGER_LOGS_WORKSHEET)

    @instrumented(log.debug)
    def addlog(*args):
        return logs_sheet.append_row(*args)

    Thread(target=addlog,
           args=([
                start,
                end,
                job_args['document'],
                job_args['sheet'],
                job_args['cellrange'],
                'Failure' if error else 'Success',
                error if error else result
                ],)
    ).start()


def filter_fixing_invalid_schedules(jobs):
    for job in jobs:
        if job['Refresh Interval']:
            try:
                is_scheduled(job)
            except Exception as e:
                update_invalid_schedule(job, e)
                continue
        yield job


def should_run(job):
    return (not job['State'] == 'Running') \
        and (job['Refresh Now'] or is_scheduled(job))


def run_job(job):
    start = update_running(job)

    args = {
            'document': job['Document'],
            'sheet': job['Sheet'],
            'cellrange': job['Range']
           }
    result, error = run_export(**args)

    if not error and job['Target System']:
        temp_file = result
        result, error = run_load(temp_file, job)
        remove(temp_file)

    if error:
        end = update_failure(job, translate_error(error, args))
    else:
        end = update_success(job, result)

    return args, result, error, start, end


@instrumented(log.info)
def run():
    while(True):
        sleep(1)
        all_jobs = read_control_sheet()
        for job in filter_fixing_invalid_schedules(all_jobs):
            if should_run(job):
                run_report = run_job(job)
                add_log_line(*run_report)


control_doc = gc.open(MANAGER_DOCUMENT)
jobs_sheet = control_doc.worksheet(MANAGER_JOBS_WORKSHEET) if MANAGER_JOBS_WORKSHEET else control_doc.sheet1

