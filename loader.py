#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import ConfigParser
import logging

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

"""Инициализируем модуль для работы с конфигами"""
config = ConfigParser.ConfigParser()
"""Подгружаем конфиг"""
config.read('loader.cfg')

"""Получаем основные переменные"""
user = config.get("Oracle", "user")
password = config.get("Oracle", "password")
server = config.get("Oracle", "server")
port = config.get("Oracle", "port")
instance = config.get("Oracle", "instance")

"""Инициализируем систему логирования"""
level_name = config.get("Logs", "level")
level = LEVELS.get(level_name, logging.NOTSET)
logging.basicConfig(level = level,
		    format = config.get("Logs", "format", 1),
		    filename = config.get("Logs", "filename"),
		    filemode = 'w')

"""Создаём логгеры"""
main_logger = logging.getLogger('Main')
orcl_logger = logging.getLogger('Oracle')
file_logger = logging.getLogger('Files')

main_logger.info("Starting mp3loader")

"""Пробуем подключить модуль cx_Oracle"""
main_logger.debug("Try import cx_Oracle")
try:
    import cx_Oracle
except ImportError, info:
    print "Import Error: ", info
    main_logger.critical("Import Error: %s", info)
    sys.exit()

"""Проверяем версию модуля"""
main_logger.debug("Check cx_Oracle version")
if cx_Oracle.version < '3.0':
    print "Very old version of cx_Oracle: ", cx_Oracle.version
    main_logger.critical("Very old version of cx_Oracle: %s", cx_Oracle.version)
    sys.exit()

"""Пробуем подключиться к базе данных"""
try:
    print "Connecting to Mobisky.."
    orcl_logger.info("Connecting to Mobisky.")
    my_connection = cx_Oracle.connect(user + '/' + password + '@//' + server + ':' + port + '/' +instance)
except cx_Oracle.DatabaseError, info:
    print "Logon Error:", info
    orcl_logger.error("Logon Error: %s", info)
    exit(0)

"""Открываем курсор"""
orcl_logger.debug("Opening cursor.")
my_cursor = my_connection.cursor()

"""Проверяем, есть ли таблица"""
SQL = """SELECT COUNT(*)
           INTO :p_Value
           FROM USER_OBJECTS
          WHERE OBJECT_NAME = 'MSY_CONTENT_MP3_TMP'
          AND OBJECT_TYPE = 'TABLE';"""

var = my_cursor.var(cx_Oracle.NUMBER)

try:
    orcl_logger.debug("Check MSY_CONTENT_MP3_TMP is exist.")
    my_cursor.execute(SQL, p_Value = var)
except cx_Oracle.DatabaseError, info:
    print "SQL Error:", info
    orcl_logger.error("SQL Error: %s", info)
    exit(0)

COUNT = var.getvalue()

if COUNT:
    """Удаляем временную таблицу"""
    SQL = "TRUNCATE MSY_CONTENT_MP3_TMP;"

    try:
        orcl_logger.debug("Delete temporaly table")
        my_cursor.execute(SQL)
    except cx_Oracle.DatabaseError, info:
        print "SQL Error:", info
        orcl_logger.error("SQL Error: %s", info)
        exit(0)

"""Создаём временную таблицу"""
SQL = """CREATE TABLE MSY_CONTENT_MP3_TMP
         (
             CODE VARCHAR2(4000 BYTE)
         );"""

try:
    orcl_logger.debug("Create temporaly table")
    my_cursor.execute(SQL)
except cx_Oracle.DatabaseError, info:
    print "SQL Error:", info
    orcl_logger.error("SQL Error: %s", info)
    exit(0)

"""Копируем из внешней таблицы во временную"""
SQL = """INSERT INTO MSY_CONTENT_MP3_TMP (CODE)
            SELECT CODE FROM MSY_CONTENT_MP3_EXT;"""

try:
    orcl_logger.debug("Copy data from external table into temporaly")
    my_cursor.execute(SQL)
except cx_Oracle.DatabaseError, info:
    print "SQL Error:", info
    orcl_logger.error("SQL Error: %s", info)
    exit(0)


"""Проверяем коды в существуюущих"""
SQL = """SELECT CONTENT_ID FROM CONTENT WHERE CODE IN (
            SELECT CODE FROM MSY_CONTENT_MP3_TMP);"""

try:
    orcl_logger.debug("Check CODE in CONTENT")
    my_cursor.execute(SQL)
except cx_Oracle.DatabaseError, info:
    print "SQL Error:", info
    orcl_logger.error("SQL Error: %s", info)
    exit(0)

for record in my_cursor.fetchall():
    print record

"""Выбираем варианты контента у дублирующихся"""
SQL = "SELECT * FROM CONTENT_VARIANTS WHERE CONTENT_ID = :ID;"

"""Копируем из временной таблицы в MSY_CONTENT_MP3"""
SQL = """INSERT INTO MSY_CONTENT_MP3 (CODE)
            SELECT CODE FROM MSY_CONTENT_MP3_TMP
             WHERE CODE NOT IN ('402884','402652','402669','402682');"""

"""Добавляем в контент"""
SQL = """DECLARE
           N_CONTENT_ID CONTENT.ID%TYPE;
         BEGIN

            FOR REC IN(
               SELECT CODE FROM MSY_CONTENT_TMP
                WHERE CODE NOT IN ('402884','402652','402669','402682');
            )
            LOOP

                INSERT INTO CONTENT(CODE, TARIFF_CLASS_ID, CONTENT_GROUP_ID, NAME, IS_ENABLED,
                                IS_VISIBLE, IS_NEW, CREATE_DATE, UPDATE_DATE, WAP_FILE_NAME)
                            VALUES (REC.CODE, 0, 64054, REC.CODE, 1,
                                1, 1, SYSDATE, SYSDATE, REC.CODE||'.mp3')
                RETURNING ID INTO N_CONTENT_ID;

                INSERT INTO CONTENT_VARIANTS(CONTENT_ID, CONTENT_SUBTYPE_ID,FILE_NAME, REMOTE_URL)
                            VALUES(N_CONTENT_ID, 11, REC.CODE||'.mp3','http://127.0.0.1/downloads/'||REC.CODE||'.mp3');
            END LOOP;
         END;"""

"""Пробуем выполнить запрос"""
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
    orcl_logger.error("SQL Error: %s", info)
    exit(0)

orcl_logger.info('Database: %s', my_connection.tnsentry)

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
