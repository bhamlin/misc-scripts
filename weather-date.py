#!/usr/bin/python3

from argparse import ArgumentParser, SUPPRESS
from collections import namedtuple
from datetime import datetime, timedelta
from urllib.request import urlopen
import csv
import postgresql

URL_BASE = ('http://www.wunderground.com/history/airport/{0}/{1[0]}/{1[1]}/{1[2]}/'
    + 'DailyHistory.html?format=1')
FIELDS = ('TimeEDT,TemperatureF,DewPointF,Humidity,SeaLevelPressureIn,'
    + 'VisibilityMPH,WindDirection,WindSpeedMPH,GustSpeedMPH,PrecipitationIn,'
    + 'Events,Conditions,WindDirDegrees,DateUTC')

CSV_DATA_LINE = namedtuple('CSV_DATA_LINE', FIELDS.split(','))

parser = ArgumentParser()
parser.add_argument('source')
parser.add_argument('date', nargs='?')

args = parser.parse_args()
location = args.source.upper()

if args.date:
    yesterday = datetime.strptime(args.date, '%Y-%m-%d')
else:
    yesterday = datetime.now().date() - timedelta(days=1)

url = URL_BASE.format(location, tuple(map(int, str(yesterday)[:10].split('-'))))

#print(url)

data = urlopen(url)
#output = open('temp', 'wb')
#data = open('temp', 'rb')
temp_data = []

# TimeEDT, TemperatureF, DewPointF, Humidity, SeaLevelPressureIn, VisibilityMPH,
#   WindDirection, WindSpeedMPH, GustSpeedMPH, PrecipitationIn, Events, Conditions,
#   WindDirDegrees, DateUTC
for line_raw in data:
    line = line_raw.decode().strip()
    if ':' in line and ',' in line:
        data_line = CSV_DATA_LINE(*line[:-6].split(','))
        temp_data.append(data_line)

def get_id(get_query, put_query, value, first_try=True):
    result = get_query.first(value)
    if first_try and not result:
        put_query(value)
        result = get_id(get_query, put_query, value, first_try=False)
    return result

db = postgresql.open(
    'pq://maintenance:@warehouse.brec.local/Black River?search_path=weather&timezone=utc',
    password='[REDACTED]')

pq_get_conditions_id = db.prepare('select id from weather.conditions where "desc" = $1')
pg_put_conditions_id = db.prepare('insert into weather.conditions ("desc") values ($1)')
get_conditions_id = lambda v: get_id(pq_get_conditions_id, pg_put_conditions_id, v)

pq_get_events_id = db.prepare('select id from weather.events where "desc" = $1')
pg_put_events_id = db.prepare('insert into weather.events ("desc") values ($1)')
get_events_id = lambda v: get_id(pq_get_events_id, pg_put_events_id, v)

pq_get_wind_dir_id = db.prepare('select id from weather.wind_direction where "desc" = $1')
pg_put_wind_dir_id = db.prepare('insert into weather.wind_direction ("desc") values ($1)')
get_wind_dir_id = lambda v: get_id(pq_get_wind_dir_id, pg_put_wind_dir_id, v)

pg_get_time_exists = db.prepare('''
select count(id) > 0 from weather.sumter_data
where measurement_time = $1::text::timestamp with time zone
'''.strip())
get_time_exists = lambda v: pg_get_time_exists.first(v)

pq_put_record = db.prepare('''
insert into weather.sumter_data (
    measurement_time, temperature, dew_point, humidity, sea_level_pressure, 
    visibility, id_wind_direction, wind_speed, gust_speed, precipitation, 
    id_events, id_conditions, wind_direction
) values (
    $1::text::timestamp, $2::text::double precision, $3::text::double precision,
    $4::text::double precision, $5::text::double precision, $6::text::double precision,
    $7, $8::text::double precision, $9::text::double precision,
    $10::text::double precision, $11, $12, $13::text::double precision
)
    '''.strip())

for row in temp_data:
    if not get_time_exists(row.DateUTC):
        print('Adding', row.DateUTC, '->', row.TemperatureF, 'degF')
        humidity = row.Humidity
        if humidity == 'N/A':
            humidity = '0.0';
        pq_put_record(
            row.DateUTC, row.TemperatureF, row.DewPointF, humidity,
            row.SeaLevelPressureIn, row.VisibilityMPH,
            get_wind_dir_id(row.WindDirection),
            row.WindSpeedMPH if row.WindSpeedMPH != 'Calm' else '0.0',
            row.GustSpeedMPH if row.GustSpeedMPH != '-' else '0.0',
            row.PrecipitationIn if row.PrecipitationIn != 'N/A' else '0.0',
            get_events_id(row.Events), get_conditions_id(row.Conditions),
            row.WindDirDegrees)
    else:
        print('Exists', row.DateUTC, '->', row.TemperatureF, 'degF')

db.close()
