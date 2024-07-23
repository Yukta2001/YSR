import pandas as pd
import psycopg2
import oracledb

# Oracle connection function
def get_oracle_connection():
    user_name = 'ARGSRI_FINAL' 
    password_var = 'ysrStg#423'
    hostname = '10.10.223.179'
    port = 1521
    sid = 'sysrasdb'
    
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
au.user_id as emp_code,ah.hosp_id,ah.hosp_name,ah.dist_id hosp_dist_id,
al.loc_name hosp_dist_name,
aeh.mandal as hosp_mandal_id,
(SELECT LOC_NAME FROM ASRIM_LOCATIONS WHERE aeh.mandal = loc_id) hospital_mandal,
CASE WHEN NEW_EMP_CODE IS NOT NULL THEN new_emp_code
ELSE login_name END as login_id,encripted_password,au.ACTIVE_YN
FROM asrim_nwh_users anu
JOIN asrim_users au ON anu.user_id=au.user_id
JOIN asrim_hospitals ah ON anu.hosp_id = ah.hosp_id
LEFT JOIN asrim_locations al ON ah.dist_id=al.loc_id
LEFT join ASRIT_EMPNL_HOSPINFO AEH on AH.HOSP_EMPNL_REF_NUM = AEH.HOSPINFO_ID
WHERE au.PRIMARY_FLAG = 'Y' 
AND anu.eff_end_dt is null
AND (au.crt_dt >= trunc(sysdate) - 1 OR au.lst_upd_dt >= trunc(sysdate) - 1)'''
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
print(df)

# Close Oracle cursor
cursor.close()

print("Before creating PostgreSQL connection")
# PostgreSQL Connection
postgres_conn = create_postgres_connection()
print("After creating PostgreSQL connection")

# Function to update password in ysr.tmp_user_mst if it's changed
def update_password_if_changed(conn, login, new_password):
    try:
        cursor = conn.cursor()
        # Fetch the current password from PostgreSQL
        query = "SELECT pswd FROM ysr.tmp_user_mst WHERE login_name = %s"
        cursor.execute(query, (login,))
        current_password = cursor.fetchone()[0]

        # Check if the new password is different from the current password
        if current_password != new_password:
            update_query = "UPDATE ysr.tmp_user_mst SET pswd = %s WHERE login_name = %s"
            cursor.execute(update_query, (new_password, login))
            conn.commit()
            print(f"Password for LOGIN ID {login} has been updated.")
        cursor.close()
    except psycopg2.Error as e:
        conn.rollback()
        print("Error updating password:", e)

# Separate DataFrames for existing and non-existing logins
existing_login = []
non_existing_login = []

# Iterate through the DataFrame and check if login_id already exists in the database
for index, row in df.iterrows():
    login = row['LOGIN_ID']
    user = row['USER_NAME']
    enc_password = row['ENCRIPTED_PASSWORD']

    cursor = postgres_conn.cursor()
    query = "SELECT login_name FROM ysr.tmp_user_mst WHERE login_name = %s"
    cursor.execute(query, (login,))
    result = cursor.fetchone()

    if result:
        existing_login.append((login, enc_password))
    else:
        non_existing_login.append((login, user, enc_password))
        print(f"LOGIN ID {login} doesn't exist in the database")

# Function to execute insert queries for non-existing logins
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
    # Insert non-existing logins
    execute_postgres_insert_queries(postgres_conn, non_existing_login)

    # Update existing logins if the password has changed
    for login, enc_password in existing_login:
        update_password_if_changed(postgres_conn, login, enc_password)

    # Set the status to success if all operations were successful
    status = "success"
except Exception as ex:
    # If any exception occurred during the execution, set status to failure
    status = "failure"
    print("An unexpected error occurred:", ex)
    # You may include the error message in the status insertion
    error_message = str(ex)
finally:
    if postgres_conn:
        postgres_conn.close()
