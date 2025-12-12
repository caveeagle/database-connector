
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

##############################################################

try:
    with mariadb.connect(**config) as conn:

        cur = conn.cursor(dictionary=True)
        
        ###################################################

        cur.execute(f"SELECT COUNT(*) AS total FROM {TABLE_NAME}")
        total_rows_begin = cur.fetchone()['total']
        
        if(1):
        
            cur.execute(
                f'''
                INSERT INTO {TABLE_NAME} (quote, author)
                VALUES (?, ?)
                ''',
                ('Life is hard - but short!', 'King Cave I')
            )
            
            # print(cur.statement)
            
            conn.commit()
    
            inserted_id = cur.lastrowid
    
            if inserted_id is None or inserted_id == 0:
                raise RuntimeError("INSERT failed")
            
            print(f'Inserted row ID: {inserted_id}')
    
            cur.execute(f"SELECT COUNT(*) AS total FROM {TABLE_NAME}")
            total_rows_end = cur.fetchone()['total']
            
            total_rows_changed = total_rows_end - total_rows_begin
            
            print(f'Number of Inserted rows: {total_rows_changed}')
        
        ###################################################
        
        if(1):
            
            print('\nWith many rows:\n')
            
            cur.execute(
                f'SELECT * FROM {TABLE_NAME} WHERE ID = ?',
                (inserted_id,)
            )
    
            rows = cur.fetchall()
            
            rows_count = len(rows)
    
            if rows_count == 1:
                # print("Exactly one row found")
                pass
            elif rows_count == 0:
                raise RuntimeError('SELECT failed - NO ROWS FOUND')
            else:
                print(f"Unexpected number of rows: {count}")
    
            ###
    
            for row in rows:
                print(f"'{row['quote']}' --- {row['author']}")
        
        if(1):
            
            print('\nWith one row:\n')
            
            cur.execute(
                f'SELECT * FROM {TABLE_NAME} WHERE ID = ?',
                (inserted_id,)
            )

            row = cur.fetchone()
            
            if row is None:
                raise RuntimeError('SELECT failed - NO ROWS FOUND')

            print(f"'{row['quote']}' --- {row['author']}")

        
        ###################################################
        
        if(1):
        
            cur.execute(
                f'''
                UPDATE {TABLE_NAME}
                SET author = ?
                WHERE ID = ?
                ''',
                ('King Cave the First', inserted_id)
            )
            
            row_count = cur.rowcount  # Just after execute!
            
            conn.commit()
            
            if row_count == 1:
                
                print(f'\nUPDATE successful:')
                
                cur.execute(f"SELECT * FROM {TABLE_NAME} WHERE ID = {inserted_id}")
                
                row = cur.fetchone()
                
                print(f"Author: {row['author']}")
                
            else:
                
                raise RuntimeError(f'UPDATE failed or no rows updated {cur.rowcount}')                
            
        ###################################################
            
        if(1):
            
            cur.execute(
                f'''
                DELETE FROM {TABLE_NAME}
                WHERE ID = ?
                ''',
                (inserted_id,)
            )

            row_count = cur.rowcount  # Just after execute!
    
            conn.commit()
            
            ##########
            
            assert( row_count == 1 )  # First check
            
            cur.execute(f'SELECT 1 FROM {TABLE_NAME} WHERE ID = ?', (inserted_id,))
            
            assert( cur.fetchone() is None )  # Second check

            cur.execute(f"SELECT COUNT(*) AS total FROM {TABLE_NAME}")
            
            total_rows_end = cur.fetchone()['total']
            
            assert(total_rows_end == total_rows_begin)  # Third check
            
            print(f'\nRow deleted, DB cleared.')
                
            ##########
            
except mariadb.Error as e:
    print(f'MariaDB error: {e}')

print('\nAll jobs finished!')
   