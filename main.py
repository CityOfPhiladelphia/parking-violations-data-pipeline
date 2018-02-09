import csv
import sys
import logging
from copy import copy
from decimal import Decimal
from datetime import datetime
from collections import OrderedDict

import petl
import click
from smart_open import smart_open
from passyunk.parser import PassyunkParser

def get_logging():
    logger = logging.getLogger()
    handler = logging.StreamHandler(stream=sys.stderr)
    formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # def exception_handler(type, value, tb):
    #     logger.exception("Uncaught exception: {0}".format(str(value)))

    # sys.excepthook = exception_handler

    return logger

logger = None

plates = None
plates_counter = None
ticket_numbers = None
ticket_numbers_counter = None
centriods = None
dedup = False
geocode_stats = {
    'total': 0,
    'success': 0,
    'gps': 0,
    'zip': 0,
    'failed_address': 0,
    'failed_segment': 0,
    'failed_segments': set()
}

passyunk_parser = PassyunkParser()

fieldmap = OrderedDict([
    ('anon_ticket_number', {
        'type': 'string',
        'start_pos': 0,
        'end_pos': 11
    }),
    ('issue_datetime', {
        'type': 'datetime',
        'start_pos': 11,
        'end_pos': 26
    }),
    ('state', {
        'type': 'string',
        'start_pos': 26,
        'end_pos': 28
    }),
    ('anon_plate_id', {
        'type': 'string',
        'start_pos': 28,
        'end_pos': 36
    }),
    ('division', {
        'type': 'string',
        'start_pos': 36,
        'end_pos': 40
    }),
    ('location', {
        'type': 'string',
        'start_pos': 40,
        'end_pos': 75
    }),
    ('violation_desc', {
        'type': 'string',
        'start_pos': 75,
        'end_pos': 95
    }),
    ('fine', {
        'type': 'decimal',
        'start_pos': 95,
        'end_pos': 104
    }),
    ('issuing_agency', {
        'type': 'string',
        'start_pos': 104,
        'end_pos': 112
    }),
    ('lat', {
        'type': 'latlon',
        'start_pos': 112,
        'end_pos': 122
    }),
    ('lon', {
        'type': 'latlon',
        'start_pos': 122,
        'end_pos': 132
    }),
    ('gps', {
        'type': 'boolean'
    }),
    ('zip_code', {
        'type': 'string'
    })
])

headers = list(fieldmap.keys())

def parse_lat_lon(value):
    value = float(value)
    if value == 0.0:
        return None
    return value

def parse_datetime(value):
    if value[-5:] == '24:00':
        value = value[:-5] + '23:59'
    return datetime.strptime(value, '%m/%d/%Y%H:%M')

typemap = {
    'integer': int,
    'string': lambda x: x.strip(),
    'decimal': lambda x: Decimal(x.replace(',','')),
    'numeric': float,
    'datetime': parse_datetime,
    'latlon': parse_lat_lon
}

def get_transform_row(latlon_input):
    def transform_row(line):
        row = OrderedDict()
        for field in fieldmap.keys():
            config = fieldmap[field]
            if config['type'] == 'latlon' and not latlon_input:
                row[field] = None
            elif 'start_pos' in config and 'end_pos' in config:
                row[field] = typemap[config['type']]\
                                (line[0][config['start_pos']:config['end_pos']])
            else:
                row[field] = None

        if not latlon_input:
            row['lat'] = None
            row['lon'] = None

        if (row['division'] == '0000' or row['division'] == '00'):
            row['division'] = None

        if row['issuing_agency'] == 'RED LI':
            row['issuing_agency'] = 'PRISON'

        if row['issuing_agency'] != 'PPA':
            row['division'] = None

        return row.values()

    return transform_row

def anonymize(row):
    global plates_counter, ticket_numbers_counter

    out_row = list(row)

    plate_key = row[2] + row[3]
    if plate_key in plates:
        out_row[3] = plates[plate_key]['id']
    else:
        plates_counter += 1
        plates[plate_key] = {
            'id': plates_counter,
            'plate': row[2],
            'state': row[3],
            'date_added': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        out_row[3] = plates_counter

    ticket_number = row[0]
    if ticket_number in ticket_numbers:
        if dedup is True:
            ticket_numbers_counter += 1
            ticket_numbers[ticket_number + 'E'] = ticket_numbers_counter
            logger.info('Duplicate detected on ticket number {} - Adding {}E'.format(
                ticket_number, ticket_number))
        else:
            out_row[0] = ticket_numbers[ticket_number]
    else:
        ticket_numbers_counter += 1
        ticket_numbers[ticket_number] = ticket_numbers_counter
        out_row[0] = ticket_numbers_counter

    return out_row

def geocode(row):
    global geocode_stats

    out_row = list(row)

    address_components = passyunk_parser.parse(row[5])
    out_row[5] = address_components['components']['output_address']
    out_row[12] = address_components['components']['mailing']['zipcode']
    if out_row[12]:
        geocode_stats['zip'] += 1

    geocode_stats['total'] += 1

    ## No GPS lat/lon
    if row[9] == None:
        if address_components['components']['cl_seg_id'] != None and \
           address_components['components']['cl_seg_id'] in centriods:
            lat_lon = centriods[address_components['components']['cl_seg_id']]
            out_row[9] = lat_lon['Lat']
            out_row[10] = lat_lon['lon']
            out_row[11] = False
            geocode_stats['success'] += 1
        else:
            if address_components['components']['cl_seg_id'] != None:
                geocode_stats['failed_segment'] += 1
                segment = address_components['components']['cl_seg_id']
            else:
                geocode_stats['failed_address'] += 1
                segment = None

            if segment:
                geocode_stats['failed_segments'].add(segment)
            logger.info('Geocode - {} not found'.format(segment or row[5]))
    else:
        geocode_stats['gps'] += 1
        out_row[11] = True

    return out_row

def load_index_file(path, _type):
    if path == None:
        return {}

    with smart_open(path, 'r') as file:
        reader = csv.DictReader(file)

        index = {}
        for row in reader:
            if _type == 'plates_file':
                index[row['state'] + row['plate']] = row
            elif _type == 'license_file':
                index[row['ticket_number']] = row['anon_ticket_number']
            elif _type == 'centriod_file':
                index[row['SEG_ID']] = row
            else:
                raise Exception('`{}` not a supported index file type'.format(_type))

        return index

def save_index_file(path, index, headers, _type):
    if path:
        with smart_open(path, 'w') as file:
            writer = csv.DictWriter(file, fieldnames=headers)

            writer.writeheader()

            for key, value in index.items():
                if _type == 'plates_file':
                    writer.writerow(value)
                elif _type == 'license_file':
                    writer.writerow({'ticket_number': key, 'anon_ticket_number': value})
                else:
                    raise Exception('`{}` not a supported index file type'.format(_type))

@click.command()
@click.option('--plates-file')
@click.option('--ticket-numbers-file')
@click.option('--centriod-file')
@click.option('--latlon-input/--no-latlon-input', is_flag=True, default=True)
@click.option('--deduplicate/--no-deduplicate', is_flag=True, default=False, help="Add 'E' to the end of duplicate ticket numbers")
def main(plates_file, ticket_numbers_file, centriod_file, latlon_input, deduplicate):
    global logger, plates, plates_counter, ticket_numbers, ticket_numbers_counter, centriods, dedup

    logger = get_logging()

    dedup = deduplicate

    plates = load_index_file(plates_file, 'plates_file')
    plate_numbers_values = plates.values()
    if len(plate_numbers_values) > 0:
        plates_counter = max(map(lambda x: int(x['id']), plate_numbers_values))
    else:
        plates_counter = 0

    logger.info('Plates autoincrement starting at: {}'.format(plates_counter))

    ticket_numbers = load_index_file(ticket_numbers_file, 'license_file')
    ticket_numbers_values = ticket_numbers.values()
    if len(ticket_numbers_values) > 0:
        ticket_numbers_counter = max(map(lambda x: int(x), ticket_numbers_values))
    else:
        ticket_numbers_counter = 0

    logger.info('Ticket number autoincrement starting at: {}'.format(ticket_numbers_counter))

    centriods = load_index_file(centriod_file, 'centriod_file')

    (
        petl
        .fromtext(strip=False)
        .rowmap(get_transform_row(latlon_input), header=headers, failonerror=True)
        .select('{fine} > 0.0 and {issue_datetime} >= 2012-01-01T00:00:00')
        .rowmap(anonymize, header=headers, failonerror=True)
        .rowmap(geocode, header=headers, failonerror=True)
        .tocsv()
    )

    logger.info('Geocode stats - success rate: {:.2%}, successes: {}, gps: {}, zip: {}, failed_segment: {}, failed_address: {}'.format(
        geocode_stats['success'] / geocode_stats['total'],
        geocode_stats['success'],
        geocode_stats['gps'],
        geocode_stats['zip'],
        geocode_stats['failed_segment'],
        geocode_stats['failed_address']))

    if len(geocode_stats['failed_segments']) > 0:
        logger.info('Failed Segments - {}'.format(','.join(geocode_stats['failed_segments'])))

    save_index_file(plates_file,
                    plates,
                    ['id',
                     'plate',
                     'state',
                     'date_added'],
                    'plates_file')
    save_index_file(ticket_numbers_file,
                    ticket_numbers,
                    ['ticket_number', 'anon_ticket_number'],
                    'license_file')

if __name__ == '__main__':
    main()
