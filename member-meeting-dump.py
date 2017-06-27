#!/usr/bin/python3

import os, sys

os.environ['TNS_ADMIN'] = '/usr/lib/oracle/11.2/client'

import cx_Oracle

year = sys.argv[1]

ats_db = cx_Oracle.connect('rpt_admin/[REDACTED]@ats')
ats_cur = ats_db.cursor()

ats_cur.execute('select memberno, cast(DTS as timestamp) as regtime ' +
    ' from cisdata.member_annual_meeting where meetingyear = :arg_date order by DTS',
    arg_date=year)
for data in ats_cur.fetchall():
    print(data[0], str(data[1]).replace(' ', 'T'), sep=',')

try:
    ats_cur.close()
    ats_db.close()
except:
    pass
