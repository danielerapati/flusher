import datetime

import arrow


VALID_TIMERANGES = ['day', 'hour', 'minute']
INVALID_TIMERANGES = ['second', 'microsecond', 'week', 'month']


def _clean(string):
    return ''.join([l for l in string.lower()
                    .replace('days', 'day')
                    .replace('hours', 'hour')
                    .replace('minutes', 'minute')
                    if l.isalnum() or l.isspace()])


def from_human(string):
    human = _clean(string)
    if any(timerange in human for timerange in INVALID_TIMERANGES):
        raise NotImplementedError('Only minutes, hours and days supported in Refresh Interval.')
    if not any(timerange in human for timerange in VALID_TIMERANGES):
        raise NotImplementedError('Something wrong with your Refresh Interval, try something like "2 minutes".')

    parts = human.split()

    days = int(parts[parts.index('day') - 1]) if 'day' in parts else 0
    hours = int(parts[parts.index('hour') - 1]) if 'hour' in parts else 0
    minutes = int(parts[parts.index('minute') - 1]) if 'minute' in parts else 0

    return datetime.timedelta(days=days, hours=hours, minutes=minutes)


def is_scheduled(job):
    if job.get('Refresh Interval'):
        last_run = arrow.get(job['Last Success']) if job['Last Success'] else arrow.get('1900-01-01')
        interval = from_human(job['Refresh Interval'])
        return arrow.utcnow() - last_run > interval
