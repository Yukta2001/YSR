import pandas as pd
import psycopg2
import oracledb
import datetime

# Oracle connection function
def get_oracle_connection():
    user_name = 'ARGSRI_FINAL' 
    password_var = 'ntrvs#123'
    hostname = '10.9.39.218'
    port = 1521
    sid = 'pysrasdb'
    
    connection = oracledb.connect(user=user_name, password=password_var,
                                  host=hostname, port=port, sid=sid)
    cursor = connection.cursor()
    return cursor

# PostgreSQL connection function
def create_postgres_connection():
    try:
        # Define connection parameters
        dbname = "ysr_db"
        user = "ysr_manager"
        password = "ysr_user_manager"
        host = "172.16.32.168"
        port = "5432"
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        
        print("Connected to PostgreSQL")
        return conn
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL database:", e)
        return None
    except Exception as ex:
        print("An unexpected error occurred:", ex)
        return None

# Oracle DB Query and DataFrame Creation
cursor = get_oracle_connection()
query = '''SELECT to_char(first_name||' '||last_name) as user_name,
CASE WHEN NEW_EMP_CODE IS NOT NULL THEN new_emp_code
ELSE login_name END as login_id,encripted_password 
FROM asrim_users WHERE ACTIVE_YN = 'Y' AND PRIMARY_FLAG = 'Y' FETCH NEXT 10 ROWS ONLY'''
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
print(df)

# Close Oracle cursor
cursor.close()

print("Before creating PostgreSQL connection")
# PostgreSQL Connection
postgres_conn = create_postgres_connection()
print("After creating PostgreSQL connection")
   
# Function to check if login_id exists in ysr.ysr_user_mst
def check_login_id_exists(conn, login):
    try:
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM ysr.tmp_user_mst WHERE login_name = %s"
        cursor.execute(query, (login,))
        count = cursor.fetchone()[0]
        return count > 0  # Return True if login_id exists, False otherwise
    except psycopg2.Error as e:
        print("Error checking existence:", e)
        return False  # Return False in case of an error

# Separate DataFrames for existing and non-existing logins
existing_login = []
non_existing_login = []
update_login = []

# Iterate through the DataFrame and append values from the login_id to the list to take only the existing logins
for index, row in df.iterrows():
    existing_login.append(row['LOGIN_ID'])
# Print the list
print('EXISTING LOGINS')
print(existing_login)

# Iterate through the DataFrame and check if login_id already exists
for index, row in df.iterrows():
    login = row['LOGIN_ID']
    user = row['USER_NAME']
    enc_password = row['ENCRIPTED_PASSWORD']

    if check_login_id_exists(postgres_conn, login):
        # Fetch existing bank details from PostgreSQL
        cursor = postgres_conn.cursor()
        query = "SELECT login_name,user_name, pswd,CASE WHEN is_active = true THEN 'Yes' ELSE 'No' END AS active_yn FROM ysr.tmp_user_mst WHERE login_name = %s"
        cursor.execute(query, (login,))
        existing_login_details = cursor.fetchone()

        ## Convert is_active values from string to boolean
        if existing_login_details is not None:
            if existing_login_details[0] != login or existing_login_details[3] != 'Yes':
                update_login.append((login, user, enc_password))
                print(f"LOGIN ID {login} requires update")
    else:
        non_existing_login.append((login, user, enc_password))
        print(f"LOGIN ID {login} doesn't exist in the database")

def update_inactive_login_flag(postgres_conn, existing_login_ids):
    try:
        cursor = postgres_conn.cursor()
        
        # Fetch all bank IDs from ysr.tmp_user_mst 
        cursor.execute("SELECT login_name FROM ysr.tmp_user_mst WHERE is_active = true")
        all_login_ids = [row[0] for row in cursor.fetchall()]
        existing_login_ids = [login for login in existing_login_ids]
        # Print the bank IDs fetched
        print("ALL LOGIN IDS:", all_login_ids)
        print("EXISTING LOGIN IDS:", existing_login_ids)

#         # Find bank IDs present in all_bank_ids but not in existing_bank_ids
        inactive_login_ids = [x for x in all_login_ids if x not in existing_login_ids]
        print("inactive:",inactive_login_ids)

#         # If there are no inactive bank IDs, return without updating
        if not inactive_login_ids:
            print("No inactive logins found, skipping update")
            return

#         # Update is_active flag for inactive bank IDs
        for login_id in inactive_login_ids:
            sql_update_is_inactive = "UPDATE ysr.tmp_user_mst SET is_active = false WHERE login_name = %s"
            cursor.execute(sql_update_is_inactive, (login_id,))

        postgres_conn.commit()
        print("Inactive logins updated successfully")
    except psycopg2.Error as e:
        postgres_conn.rollback()
        print("Error updating inactive logins:", e)
    finally:
        cursor.close()


# Function to execute insert queries for non-existing banks
def execute_postgres_insert_queries(conn, insert_queries):
    try:
        cursor = conn.cursor()
        for login, user, enc_password in insert_queries:
            sql_insert_login = "INSERT INTO ysr.tmp_user_mst (login_name, user_name, pswd) VALUES (%s, %s, %s)"
            cursor.execute(sql_insert_login, (login, user, enc_password))
        conn.commit()
        print("Data inserted successfully")
    except psycopg2.Error as e:
        conn.rollback()
        print("Error inserting data:", e)
    finally:
        cursor.close()


# # Function to execute update queries for non-existing banks
def execute_postgres_update_queries(conn, insert_queries):
    try:
        cursor = conn.cursor()
        for login, user, enc_password in insert_queries:
            sql_update_login = "UPDATE ysr.tmp_user_mst SET login_name = %s,user_name = %s, pswd = %s, is_active = true WHERE login_name = %s"
            cursor.execute(sql_update_login, (login, user, enc_password,login))
        conn.commit()
        print("Data inserted successfully")
    except psycopg2.Error as e:
        conn.rollback()
        print("Error inserting data:", e)
    finally:
        cursor.close()


try:
    # After executing insert queries for non-existing logins
    execute_postgres_insert_queries(postgres_conn, non_existing_login)

#     # After executing update queries for existing banks
    execute_postgres_update_queries(postgres_conn, update_login)

#     # Call the function to update inactive banks flag
    update_inactive_login_flag(postgres_conn, existing_login)

    # Set the status to success if all operations were successful
    status = "success"
except Exception as ex:
    # If any exception occurred during the execution, set status to failure
    status = "failure"
    print("An unexpected error occurred:", ex)
    # You may include the error message in the status insertion
    error_message = str(ex)

# def insert_scheduler_status(conn, scheduler_name, status, execution_time=None, error_msg=None):
#     try:
#         cursor = conn.cursor()
#         if execution_time is None:
#             execution_time = datetime.datetime.now()
#         cursor.execute(
#             "INSERT INTO ysr.ysr_scheduler_status (scheduler_name, status, execution_time, error_msg) "
#             "VALUES (%s, %s, %s, %s)",
#             (scheduler_name, status, execution_time, error_msg)
#         )
#         conn.commit()
#         print("Scheduler status inserted successfully")
#     except psycopg2.Error as e:
#         conn.rollback()
#         print("Error inserting scheduler status:", e)
#     finally:
#         cursor.close()

# # Insert scheduler status after all operations are done
# insert_scheduler_status(postgres_conn, "BANK MASTER SYNC", status, error_msg=error_message if status == "failure" else None)