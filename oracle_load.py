import os
import csv
import sys
import io
from datetime import datetime

import click
import cx_Oracle

TABLE_SQL = '''
CREATE TABLE "GIS_BAA"."PARKING_VIOLATIONS{}"
(
    "OBJECTID" NUMBER(*,0), 
    "ANON_TICKET_NUMBER" NUMBER(*,0),
    "ISSUE_DATETIME" TIMESTAMP,
    "STATE" VARCHAR2(8 BYTE),
    "ANON_PLATE_ID" NUMBER(*,0),
    "DIVISION" VARCHAR2(8 BYTE),
    "LOCATION" VARCHAR2(128 BYTE),
    "VIOLATION_DESC" VARCHAR2(128 BYTE),
    "FINE" NUMBER(*,0),
    "ISSUING_AGENCY" VARCHAR2(8 BYTE),
    "LAT" FLOAT(126),
    "LON" FLOAT(126),
    "GPS" SMALLINT,
    "ZIP_CODE" VARCHAR2(10 BYTE),
    "SHAPE" SDE.ST_GEOMETRY
)
'''

INSERT_SQL = '''
INSERT INTO "GIS_BAA"."PARKING_VIOLATIONS_TEMP"
  ("OBJECTID",
   "ANON_TICKET_NUMBER",
   "ISSUE_DATETIME",
   "STATE",
   "ANON_PLATE_ID", 
   "DIVISION", 
   "LOCATION", 
   "VIOLATION_DESC", 
   "FINE", 
   "ISSUING_AGENCY", 
   "LAT", 
   "LON", 
   "GPS", 
   "ZIP_CODE", 
   "SHAPE")
VALUES (:cartodb_id,
        :anon_ticket_number,
        to_timestamp_tz(:issue_datetime, 'YYYY-MM-DD HH24:MI:SS'),
        :state,
        :anon_plate_id,
        :division,
        :location,
        :violation_desc,
        :fine,
        :issuing_agency,
        :lat,
        :lon,
        :gps,
        :zip_code,
        CASE
          WHEN :shape IS NOT NULL THEN sde.st_geomfromtext(:shape, 4326)
          ELSE null
        END)
'''

DELETE_SQL = '''
DELETE FROM "GIS_BAA"."PARKING_VIOLATIONS"
WHERE "ANON_TICKET_NUMBER" IN
  (SELECT "GIS_BAA"."PARKING_VIOLATIONS"."ANON_TICKET_NUMBER" FROM "GIS_BAA"."PARKING_VIOLATIONS"
   JOIN "GIS_BAA"."PARKING_VIOLATIONS_TEMP"
     ON "GIS_BAA"."PARKING_VIOLATIONS"."ANON_TICKET_NUMBER" = "GIS_BAA"."PARKING_VIOLATIONS_TEMP"."ANON_TICKET_NUMBER")
'''

INSERT_BATCH_SQL = 'INSERT INTO "GIS_BAA"."PARKING_VIOLATIONS" SELECT * FROM "GIS_BAA"."PARKING_VIOLATIONS_TEMP"'
CLEAR_TEMP_TABLE_SQL = 'DELETE FROM "GIS_BAA"."PARKING_VIOLATIONS_TEMP"'

def upsert_table(conn):
    cur = conn.cursor()
    cur.execute(TABLE_SQL.format(''))
    cur.close()

def upsert_temp_table(conn):
    click.echo(TABLE_SQL.format('_TEMP'))
    cur = conn.cursor()
    cur.execute(TABLE_SQL.format('_TEMP'))
    cur.execute(CLEAR_TEMP_TABLE_SQL)
    conn.commit()
    curc.close()

def get_connection():
    return cx_Oracle.connect(os.getenv('ORACLE_USERNAME'),
                             os.getenv('ORACLE_PASSWORD'),
                             os.getenv('ORACLE_HOST') + '/' +
                             os.getenv('ORACLE_SERVICE'))

def upsert_batch(conn, batch, row_number, batch_size):
    batch_start = datetime.utcnow()
    click.echo('Upserting batch: {} - {}'.format((row_number - batch_size) + 1, row_number))

    conn.begin()
    cur = conn.cursor()

    def transform(row):
        row['gps'] = 1 if row['gps'] == 'true' else 0
        row['shape'] = None if row['shape'] == '' else row['shape']
        return row

    cur.prepare(INSERT_SQL)
    cur.executemany(None, list(map(transform, batch)))

    cur.execute(DELETE_SQL)
    cur.execute(INSERT_BATCH_SQL)
    cur.execute(CLEAR_TEMP_TABLE_SQL)

    cur.close()
    conn.commit()

    click.echo('Batch upserted - runtime {}s'.format((datetime.utcnow() - batch_start).total_seconds()))

@click.command()
@click.option('--starting-row', type=int, default=1)
@click.option('--batch-size', type=int, default=10000)
@click.option('--create-table', is_flag=True, default=False)
@click.option('--create-temp-table', is_flag=True, default=False)
def main(starting_row, batch_size, create_table, create_temp_table):
    conn = get_connection()

    try:
        if create_table:
            upsert_table(conn)
        if create_temp_table:
            upsert_temp_table(conn)

        input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

        reader = csv.DictReader(input_stream)

        script_start = datetime.utcnow()

        row_number = 0
        _buffer = []
        for row in reader:
            row_number += 1
            if row_number >= starting_row:
                _buffer.append(row)
                if len(_buffer) >= batch_size:
                    upsert_batch(conn, _buffer, row_number, batch_size)
                    _buffer = []

        if len(_buffer) > 0:
            upsert_batch(conn, _buffer, row_number, batch_size)

        click.echo('Total time: {}s'.format((datetime.utcnow() - script_start).total_seconds()))
    except:
        try:
            conn.rollback()
        except:
            pass
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    main()
