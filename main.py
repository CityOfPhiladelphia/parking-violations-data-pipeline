from struct import Struct, calcsize
from datetime import datetime

import petl as etl

from util import (construct_layout, get_active_header)

LAYOUT = [
    { 'start': 0,   'end': 11,  'name': 'ticket' },
    { 'start': 11,  'end': 21,  'name': 'issue_date' },
    { 'start': 21,  'end': 26,  'name': 'issue_time' },
    { 'start': 26,  'end': 28,  'name': 'state' },
    { 'start': 28,  'end': 36,  'name': 'plate' },
    { 'start': 36,  'end': 40,  'name': 'division' },
    { 'start': 40,  'end': 75,  'name': 'location' },
    { 'start': 75,  'end': 95,  'name': 'violation_desc' },
    { 'start': 95,  'end': 104, 'name': 'fine' },
    { 'start': 104, 'end': 110, 'name': 'issuing_agency' },
]

layout = construct_layout(LAYOUT)
header = get_active_header(LAYOUT)

unpack = Struct(layout).unpack_from
struct_length = calcsize(layout)

def unpack_line (line):
    packed_data = line[0] # etl.fromtext returns a list of lists w/1 item

    # Ensure string length is what deconstructer expects
    if len(packed_data) != struct_length:
        packed_data = '{:<{}s}'.format(packed_data.decode(), struct_length).encode()

    row = unpack(packed_data)

    # Trim whitespace in each field
    row = [field.strip() for field in row]

    return row

def create_date_time (row):
    date_time = row['issue_date'] + ' ' + row['issue_time']
    return datetime.strptime(date_time, '%m/%d/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')

def remove_ppa_division (division, row):
    if row['issuing_agency'] == 'PPA':
        return ''
    else:
        return division

def fix_prisons_agency (issuing_agency):
    if issuing_agency == 'RED LI':
        return 'PRISONS'
    else:
        return issuing_agency

# Main
table = etl.fromtext(strip=False)\
            .rowmap(unpack_line, header=header, failonerror=True)\
            .convert('fine', float)\
            .addfield('issue_date_and_time', create_date_time)\
            .select('{issue_date_and_time} >= "2012-01-01" and {fine} > 0')\
            .convert('division', remove_ppa_division, pass_row=True)\
            .convert('issuing_agency', fix_prisons_agency)
print(etl.dicts(table))
