
import mariadb

import config

#######################################

### Change to your own parameters ! ###

SERVER_IP       = config.SERVER_IP
DB_NAME         = config.DB_NAME
WRITE_USER      = config.WRITE_USER
WRITE_USER_PWD  = config.WRITE_USER_PWD
READ_USER       = config.READ_USER
READ_USER_PWD   = config.READ_USER_PWD


#######################################


# Параметры подключения
config = {
    'user': WRITE_USER,
    'password': WRITE_USER_PWD,
    'host': SERVER_IP,
    'database': DB_NAME,
    'port': 3306 # by default
}

TABLE_NAME = 'sample_table'

try:

    with mariadb.connect(**config) as conn:

        cur = conn.cursor(dictionary=True)

        cur.execute(f'SELECT * FROM {TABLE_NAME}')

        rows = cur.fetchall()

        for row in rows:
            print(f"{row['ID']}: \"{row['quote']}\" — {row['author']}")

except mariadb.Error as e:
    print(f"MariaDB error: {e}")

   