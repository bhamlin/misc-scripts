#!/usr/bin/python3 -tt

from datetime import date, timedelta
import os

os.environ['TNS_ADMIN'] = '/usr/lib/oracle/11.2/client'

import cx_Oracle
import postgresql

def l_(source, *message, **kwargs):
    print('[{}]'.format(source), *message, **kwargs)

def l_g(*message, **kwargs):
    l_('General', *message, **kwargs)

def l_p(*message, **kwargs):
    l_('PostgreSQL', *message, **kwargs)

def l_o(*message, **kwargs):
    l_('Oracle', *message, **kwargs)

oracle_queries = { 'main': '''

SELECT
  IM.ACCOUNTNO,
  TO_CHAR(BS.DTS, 'YYYY-MM-DD'),
  TO_CHAR(MR.READING_DATE, 'YYYY-MM-DD'),
  SUM(IM.AMOUNT),
  FLOOR(MRU.USAGE),
  TO_NUMBER(NVL(MRD.VALUE,0)),
  BS.CYCLE,
  ( TRUNC(MR.READING_DATE,'dd')-TRUNC(AU1.READING_DATE,'dd') )
 FROM
  CISDATA.INVOICE_MASTER IM,
  CISDATA.BILLING_SUMMARY BS,
  CISDATA.METERS_TO_BILL MTB,
  CISDATA.METER_READING MR,
  CISDATA.METER_READING_DETAIL MRD,
  CISDATA.RATE_ITEM_TYPES RIT,
  CISDATA.METER_READING_USAGE MRU,
  CISDATA.READING_CLASS RC,
  ATSCBS.ACCOUNT_USAGE AU1
 WHERE ( BS.DTS >= TO_DATE(:arg_date,'YYYY-MM-DD') )
  AND ( IM.INVOICENO                = MR.BILL_ID )
  AND ( IM.ACCOUNTNO                = MTB.ACCOUNTNO )
  AND ( IM.KEY                      = BS.BILL_ID )
  AND ( MR.RATE_CODE                = AU1.RATE_CODE )
  AND ( RIT.TYPE                    = AU1.TYPE )
  AND ( MTB.FROMMETERREADINGID      = AU1.READING_ID )
  AND ( MTB.BILLED                  = 'Y' )
  AND ( MR.BILLED                   = 'Y' )
  AND ( RC.CLASS_NAME              <> 'KW' )
  AND ( MTB.METERNO                 = MR.METERNO )
  AND ( MTB.TOMETERREADINGID        = MR.READING_ID )
  AND ( MR.READING_ID               = MRD.READING_ID )
  AND ( MRD.RATE_ITEM_TYPE_ID       = RIT.RATE_ITEM_TYPE_ID )
  AND ( RIT.READING_CLASS_ID        = RC.READING_CLASS_ID )
  AND ( MRD.METER_READING_DETAIL_ID = MRU.METER_READING_DETAIL_ID_TO )
 GROUP BY
  IM.ACCOUNTNO,
  BS.DTS,
  MR.READING_DATE,
  MRU.USAGE,
  TO_NUMBER(NVL(MRD.VALUE,0)),
  BS.CYCLE,
  ( TRUNC(MR.READING_DATE,'dd')-TRUNC(AU1.READING_DATE,'dd') )
 ORDER BY BS.DTS, IM.ACCOUNTNO

''', 'demand': '''

SELECT
  ACCOUNT_USAGE.ACCOUNTNO,
  TO_CHAR(ACCOUNT_USAGE.READING_DATE, 'YYYY-MM-DD'),
  SUM(ACCOUNT_USAGE.USAGE)
 FROM ATSCBS.ACCOUNT_USAGE ACCOUNT_USAGE
 WHERE ( ACCOUNT_USAGE.BILLED = 'Y' )
  AND ( ACCOUNT_USAGE.TYPE_NAME = 'kW - Demand' )
  AND ( ACCOUNT_USAGE.READING_DATE >= TO_DATE(:arg_date, 'YYYY-MM-DD') )
 GROUP BY ACCOUNT_USAGE.ACCOUNTNO, ACCOUNT_USAGE.READING_DATE,
  ACCOUNT_USAGE.TYPE_NAME
 ORDER BY
  ACCOUNT_USAGE.READING_DATE ASC,
  ACCOUNT_USAGE.ACCOUNTNO ASC,
  ACCOUNT_USAGE.TYPE_NAME ASC

'''}

del oracle_queries['main']

postgres_queries = dict()
postgres_queries_raw = { 'select_record': '''

select
  count(id) > 0
 from cis.usage_data
 where account_number = $1 and billing_date = $2

''', 'insert_record': '''

insert into cis.usage_data
    (account_number, billing_date)
  values
    ($1, $2)

''', 'update_record': '''

UPDATE cis.usage_data
  SET reading_date=$3,
    billed_amount=$4,
    billed_usage=$5,
    billed_reading=$6,
    billing_cycle=$7,
    billing_days=$8,
    billing_year=date_part('year', $2::DATE)::INTEGER,
    billing_month=date_part('month', $2::DATE)::INTEGER
  WHERE account_number=$1
    AND billing_date=$2;

''', 'update_demand': '''

UPDATE cis.usage_data
  SET billed_demand=$3
  WHERE account_number=$1
    AND reading_date=$2;

'''}

yesterday = date.today() - timedelta(9)
begin_date = str(yesterday)
begin_date = '2016-09-01'

psql_host = 'warehouse.brec.local'
psql_db = 'Black River'
psql_user = 'postgres'
psql_pass = '[REDACTED]'

ats_host = 'ats'
ats_user = 'rpt_admin'
ats_pass = '[REDACTED]'

l_g("Starting from", begin_date)

l_p('Connecting to pq://{}@{}/{}'.format(psql_user, psql_host, psql_db))
psql_db = postgresql.open(host=psql_host, database=psql_db,
        user=psql_user, password=psql_pass)

l_p('Preparing statements')
for name, query in postgres_queries_raw.items():
    postgres_queries[name] = psql_db.prepare(query)

l_o('Connecting to oracle:thin://{}@{}'.format(ats_user, ats_host))
ats_db = cx_Oracle.connect('rpt_admin/[REDACTED]@ats')
l_o('Requesting cursor')
ats_cur = ats_db.cursor()

if 'main' in oracle_queries:
    l_o('Executing query "main"')
    ats_cur.execute(oracle_queries['main'], arg_date=begin_date)
    l_o('Pulling cursor')
    last_bill_date = None
    for data in ats_cur.fetchall():
        fields = list(data)
        fields[1] = date(*list(map(int, data[1].split('-'))))
        fields[2] = date(*list(map(int, data[2].split('-'))))
        index = fields[:2]
        if last_bill_date != index[1]:
            last_bill_date = index[1]
            l_p('Processing', last_bill_date)
        entry_exists = postgres_queries['select_record'](*index)[0][0]
        if not entry_exists:
            try:
                postgres_queries['insert_record'](*index)
                postgres_queries['update_record'](*fields)
            except Exception as ex:
                print(ex, fields[:2])
        else:
            # print('Duplicate entry for', *fields[:2])
            pass
else:
    l_o('Skipping query "main"')

l_o('Executing query "demand"')
ats_cur.execute(oracle_queries['demand'], arg_date=begin_date)
l_o('Pulling cursor')
for data in ats_cur.fetchall():
    fields = list(data)
    fields[1] = date(*list(map(int, data[1].split('-'))))
    postgres_queries['update_demand'](*fields)

l_o("Done")
l_p("Done")

try:
    ats_cur.close()
    ats_db.close()
except:
    pass

try:
    #map(lambda x: x.close(), postgres_queries.values())
    for statement in postgres_queries.values():
        statement.close() if statement else None
    psql_db.close()
except:
    pass
