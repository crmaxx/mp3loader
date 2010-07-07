#!/usr/bin/python
# -*- coding: utf8 -*-

from sys import exit

try:
    import cx_Oracle
except ImportError, info:
    print "Import Error:", info
    sys.exit()

if cx_Oracle.version < '3.0':
    print "Very old version of cx_Oracle :", cx_Oracle.version
    sys.exit()

try:
    print "Connecting to Mobisky.."
    my_connection = cx_Oracle.connect('cms/cms@//mobisky_host/orcl')
except cx_Oracle.DatabaseError, info:
    print "Logon Error:", info
    exit(0)

my_cursor = my_connection.cursor()

try:
    my_cursor.execute("""
    SELECT OWNER,
           SEGMENT_TYPE,
           TABLESPACE_NAME,
           SUM(BLOCKS)SIZE_BLOCKS,
           COUNT(*) SIZE_EXTENTS
      FROM DBA_EXTENTS
     WHERE OWNER LIKE :S
     GROUP BY OWNER, SEGMENT_TYPE, TABLESPACE_NAME
    """, S = 'SYS%')
except cx_Oracle.DatabaseError, info:
    print "SQL Error:", info
    exit(0)

print
print 'Database:', my_connection.tnsentry
print
print "Used space by owner, object type, tablespace "
print "-----------------------------------------------------------"

title_mask = ('%-16s','%-16s','%-16s','%-8s','%-8s')

i = 0

for column_description in my_cursor.description:
    print title_mask[i] % column_description[0],
    i = 1 + i

print ''
print "------------------------------------------------------------"

row_mask = '%-16s %-16s %-16s %8.0f %8.0f '

for record in my_cursor.fetchall():
    print row_mask % record
