#!/usr/bin/python3 -tt

from datetime import date, timedelta
import os, sys

os.environ['TNS_ADMIN'] = '/usr/lib/oracle/11.2/client'

import cx_Oracle
import csv

query = '''

SELECT
  O237043.ACCOUNTNO as "Accountno",
  O237139.LAST_NAME as "Last Name",
  O237139.FIRST_NAME as "First Name",
  O237139.MIDDLE_NAME as "Middle Name",
  O237139.ATTN as "Attn",
  O237058.STREET_NO||' '||O237058.STREET_NAME||' '||O237058.STREET_TYPE||' '||O237058.UNIT as "Address",
  O237058.CITY as "City",
  O237058.STATE as "State",
  ATSCBS.TOZIP(O237058.ZIP) as "Zip Code",
  O237369.RATE_CODE as "Rate Code",
  ATSCBS.DISCOVERER_FUNCTIONS.GET_ATTRIBUTE_VALUE(O237043.ACCOUNTNO,'Account Attention Line') as "Attention"
FROM ATSCBS.ACCOUNT_CONTACTS O237035,
  CISDATA.ACCOUNT_MASTER O237043,
  ATSCBS.ACCOUNT_STATUS O237049,
  ATSCBS.ADDRESS O237058,
  CISDATA.CONTACTS O237139,
  CISDATA.CONTACT_CODES O237141,
  CISDATA.SERVICE_METERS O237369
WHERE ( ( O237035.ACCOUNTNO        = O237043.ACCOUNTNO )
AND ( O237043.LOCATION_ID          = O237369.LOCATION_ID(+) )
AND ( O237049.ACCOUNT_STATUS_ID    = O237043.ACCOUNT_STATUS_ID )
AND ( O237058.ADDRESS_ID           = O237035.ADDRESS_ID )
AND ( O237139.CONTACT_ID           = O237035.CONTACT_ID )
AND ( O237141.CONTACT_CODE_ID      = O237035.CONTACT_CODE_ID ) )
AND ( O237141.DESCRIPTION         IN (( 'Primary Contact' )) )
AND ( O237049.ACCOUNT_STATUS_DESC IN ('Active','Active Not Billed','Not Final') )
ORDER BY O237139.LAST_NAME ASC,
  O237139.FIRST_NAME ASC,
  O237139.MIDDLE_NAME ASC

'''.strip()

field_names = (
  "Accountno",
  "Last Name",
  "First Name",
  "Middle Name",
  "Attn",
  "Address",
  "City",
  "State",
  "Zip Code",
  "Rate Code",
  "Attention"
)

ats_host = 'ats'
ats_user = 'rpt_admin'
ats_pass = '[REDACTED]'

outfile = sys.argv[1]

ats_db = cx_Oracle.connect('{}/{}@{}'.format(ats_user, ats_pass, ats_host))
ats_cur = ats_db.cursor()

ats_cur.execute(query)

with open(outfile, 'w+') as output:
    csv_out = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
    csv_out.writerow(field_names)
    for data in ats_cur.fetchall():
        row = list(data)
        for i in range(len(row)):
            if row[i].__class__ == str:
                row[i] = row[i].strip()
        csv_out.writerow(row)

ats_cur.close()
ats_db.close()
