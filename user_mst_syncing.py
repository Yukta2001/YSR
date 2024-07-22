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
        query = "SELECT login_id,user_name, pswd,CASE WHEN is_active = true THEN 'Yes' ELSE 'No' END AS active_yn FROM ysr.tmp_user_mst WHERE login_id = %s"
        cursor.execute(query, (login,))
        existing_login = cursor.fetchone()

        if existing_login is not None:
            if existing_login[0] != login or existing_login[3] != 'Yes':
                update_login.append((login, user, enc_password))
                print(f"LOGIN ID {login} requires update")
    else:
        non_existing_login.append((login, user, enc_password))
        print(f"LOGIN ID {login} doesn't exist in the database")


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


try:
    # After executing insert queries for non-existing banks
    execute_postgres_insert_queries(postgres_conn, non_existing_login)

    # Set the status to success if all operations were successful
    status = "success"
except Exception as ex:
    # If any exception occurred during the execution, set status to failure
    status = "failure"
    print("An unexpected error occurred:", ex)
    # You may include the error message in the status insertion
    error_message = str(ex)
