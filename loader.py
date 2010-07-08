#!/usr/bin/python
# -*- coding: utf8 -*-

import sys
import ConfigParser
import logging

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

config = ConfigParser.ConfigParser()
config.read('loader.cfg')

user = config.get("Oracle", "user")
password = config.get("Oracle", "password")
server = config.get("Oracle", "server")
port = config.get("Oracle", "port")
instance = config.get("Oracle", "instance")

level_name = config.get("Logs", "level")
level = LEVELS.get(level_name, logging.NOTSET)
logging.basicConfig(level = level,
		    format = config.get("Logs", "format", 1),
		    filename = config.get("Logs", "filename"),
		    filemode = 'w')

logger = logging.getLogger('main')

try:
    import cx_Oracle
except ImportError, info:
    print "Import Error: ", info
    logger.critical("Import Error: %s", info)
    sys.exit()

if cx_Oracle.version < '3.0':
    print "Very old version of cx_Oracle: ", cx_Oracle.version
    logger.critical("Very old version of cx_Oracle: %s", cx_Oracle.version)
    sys.exit()

try:
    print "Connecting to Mobisky.."
    logger.info("Connecting to Mobisky..")
    my_connection = cx_Oracle.connect(user + '/' + password + '@//' + server + ':' + port + '/' +instance)
except cx_Oracle.DatabaseError, info:
    print "Logon Error:", info
    logger.error("Logon Error: %s", info)
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
    logger.error("SQL Error: %s", info)
    exit(0)

logger.info('Database: %s', my_connection.tnsentry)

print 'Database:', my_connection.tnsentry
print
print "Used space by owner, object type, tablespace "
print "-----------------------------------------------------------"

title_mask = ('%-16s', '%-16s', '%-16s', '%-8s', '%-8s')

i = 0

for column_description in my_cursor.description:
    print title_mask[i] % column_description[0],
    i = 1 + i

print ''
print "------------------------------------------------------------"

row_mask = '%-16s %-16s %-16s %8.0f %8.0f '

for record in my_cursor.fetchall():
    print row_mask % record
