import csv
from decimal import Decimal
from datetime import datetime
from collections import OrderedDict

import petl
import click
from passyunk.parser import PassyunkParser

passyunk_parser = PassyunkParser()

fieldmap = OrderedDict([
    ('ticket_number', {
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
    ('plate', {
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
    })
])

latlon_fieldmap = OrderedDict([
    ('lat', {
        'type': 'latlon',
        'start_pos': 112,
        'end_pos': 122
    }),
    ('lon', {
        'type': 'latlon',
        'start_pos': 122,
        'end_pos': 132
    })
])

headers = list(fieldmap.keys())
headers_with_latlon = list(fieldmap.keys() + latlon_fieldmap.keys())

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
    if latlon_input:
        input_fieldmap = fieldmap + latlon_fieldmap
    else:
        input_fieldmap = fieldmap 

    def transform_row(line):
        row = OrderedDict()
        for field in input_fieldmap.keys():
            config = input_fieldmap[field]
            row[field] = typemap[config['type']]\
                            (line[0][config['start_pos']:config['end_pos']])

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

plates = None
plates_counter = None
ticket_numbers = None
ticket_numbers_counter = None
centriods = None

def anonymize(row):
    global plates_counter, ticket_numbers_counter

    out_row = list(row)

    plate_key = row[2] + row[3]
    if plate_key in plates:
        out_row[3] = plates[plate_key]
    else:
        plates_counter += 1
        plates[plate_key] = plates_counter
        out_row[3] = plates_counter

    if row[0] in ticket_numbers:
        out_row[0] = ticket_numbers[row[0]]
    else:
        ticket_numbers_counter += 1
        ticket_numbers[row[0]] = ticket_numbers_counter
        out_row[0] = ticket_numbers_counter

    return out_row

def geocode(row):
    out_row = list(row)

    address_components = passyunk_parser.parse(row[5])
    out_row[5] = address_components['components']['output_address']

    ## No GPS lat/lon
    if row[9] == None and address_components['components']['cl_seg_id'] != None:
        if address_components['components']['cl_seg_id'] in centriods:
            lat_lon = centriods[address_components['components']['cl_seg_id']]
            out_row[9] = lat_lon[0]
            out_row[10] = lat_lon[1]
            row[10] = False
        else:
            ## TODO: !!!
            print('!!! {} not found'.format(address_components['components']['cl_seg_id']))
    else:
        row[10] = True

    return out_row

def load_index_file(path):
    if path == None:
        return {}

    ## TODO: S3 support

    ## TODO: !!! plate has multiple cols

    with open(path) as file:
        reader = csv.reader(file)
        next(reader) # skip headers

        index = {}
        for row in reader:
            if len(row) > 2:
                index[row[0]] = row[1:]
            else:
                index[row[0]] = row[1]

        return index

def save_index_file(path, index, headers):
    with open(path) as file:
        writer = csv.writer(file)

        writer.write(headers)

        for key, value in index.items():
            if isinstance(value, list):
                writer.write([key] + value)
            else:
                writer.write([key] + [value])

@click.command()
@click.option('--plates-file')
@click.option('--ticket-numbers-file')
@click.option('--centriod-file')
@click.option('--latlon-input/--no-latlon-input', is_flag=True, default=True)
def main(plates_file, ticket_numbers_file, centriod_file, latlon_input):
    global plates, plates_counter, ticket_numbers, ticket_numbers_counter, centriods

    plates = load_index_file(plates_file)
    plate_numbers_values = plates.values()
    if len(plate_numbers_values) > 0:
        plates_counter = max(plate_numbers_values)
    else:
        plates_counter = 0

    ticket_numbers = load_index_file(ticket_numbers_file)
    ticket_numbers_values = ticket_numbers.values()
    if len(ticket_numbers_values) > 0:
        ticket_numbers_counter = max(ticket_numbers_values)
    else:
        ticket_numbers_counter = 0

    centriods = load_index_file(centriod_file)

    petl\
        .fromtext(strip=False)\
        .rowmap(get_transform_row(latlon_input), header=headers_with_latlon, failonerror=True)\
        .select('{fine} > 0.0')\
        .rowmap(anonymize, header=headers_with_latlon, failonerror=True)\
        .rowmap(geocode, header=headers_with_latlon + ['gps'], failonerror=True)\
        .tocsv()

    save_index_file(plates_file, plates, ['plate_id', 'plate'])
    save_index_file(ticket_numbers_file, ticket_numbers)

if __name__ == '__main__':
    main()
