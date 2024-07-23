import pandas as pd
import psycopg2
import oracledb

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
    return connection, cursor

# PostgreSQL connection function
def create_postgres_connection():
    try:
        # Define connection parameters
        dbname = "medical_dispensary"
        user = "ntr_med_disp"
        password = "ntr_med_disp__@2024"
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
connection, cursor = get_oracle_connection()
query1 = '''SELECT to_char(first_name||' '||last_name) as user_name,
ah.hosp_id,ah.hosp_name,ah.dist_id hosp_dist_id,
al.loc_name hosp_dist_name,
aeh.mandal as hosp_mandal_id,
(SELECT LOC_NAME FROM ASRIM_LOCATIONS WHERE aeh.mandal = loc_id) hospital_mandal,
CASE WHEN NEW_EMP_CODE IS NOT NULL THEN new_emp_code
ELSE login_name END as login_id,encripted_password
FROM asrim_nwh_users anu
JOIN asrim_users au ON anu.user_id=au.user_id
JOIN asrim_hospitals ah ON anu.hosp_id = ah.hosp_id
LEFT JOIN asrim_locations al ON ah.dist_id=al.loc_id
LEFT join ASRIT_EMPNL_HOSPINFO AEH on AH.HOSP_EMPNL_REF_NUM = AEH.HOSPINFO_ID
WHERE au.PRIMARY_FLAG = 'Y' AND au.active_yn = 'Y'
AND anu.eff_end_dt is null
AND au.crt_dt >= trunc(sysdate) - 1'''

query2 = '''SELECT
ah.hosp_id,ah.hosp_name,ah.dist_id hosp_dist_id,
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
AND au.lst_upd_dt >= trunc(sysdate) - 1'''

query3 = '''SELECT user_id,hosp_id FROM asrim_nwh_users 
WHERE eff_end_dt is null 
AND crt_dt >= trunc(sysdate) - 1'''

cursor.execute(query1)
df1 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

cursor.execute(query2)
df2 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

cursor.execute(query3)
df3 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

# Close Oracle cursor
cursor.close()
connection.close()

print("Before creating PostgreSQL connection")
# PostgreSQL Connection
postgres_conn = create_postgres_connection()
print("After creating PostgreSQL connection")

# Function to check if a login_id exists in medical.md_user_mst
def check_login_exists(cursor, login_id):
    query = "SELECT COUNT(*) FROM medical.md_user_mst WHERE login_name = %s"
    cursor.execute(query, (login_id,))
    return cursor.fetchone()[0] > 0

# Function to update existing login_id details
def update_existing_login(cursor, row):
    query = '''
    UPDATE medical.md_user_mst
    SET user_name = %s, hosp_name = %s, hosp_dist_id = %s, hosp_dist_name = %s,
        hosp_mandal_id = %s, hosp_mandal_name = %s, pswd = %s
    WHERE login_name = %s
    '''
    cursor.execute(query, (
        row['USER_NAME'], row['HOSP_NAME'], row['HOSP_DIST_ID'], row['HOSP_DIST_NAME'],
        row['HOSP_MANDAL_ID'], row['HOSP_MANDAL_NAME'], row['ENCRIPTED_PASSWORD'], row['LOGIN_ID']
    ))

# Function to insert new login_id details
def insert_new_login(cursor, row):
    query = '''
    INSERT INTO medical.md_user_mst (user_name, login_name, pswd, hospital_name, hosp_id, hosp_dist_id, hosp_dist_name, hosp_mandal_id, hosp_mandal_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    cursor.execute(query, (
        row['USER_NAME'], row['LOGIN_ID'], row['ENCRIPTED_PASSWORD'], row['HOSP_NAME'],
        row['HOSP_ID'], row['HOSP_DIST_ID'], row['HOSP_DIST_NAME'], row['HOSP_MANDAL_ID'],
        row['HOSP_MANDAL_NAME']
    ))

# Insert or update records based on query1
cursor = postgres_conn.cursor()
for index, row in df1.iterrows():
    if check_login_exists(cursor, row['LOGIN_ID']):
        update_existing_login(cursor, row)
    else:
        insert_new_login(cursor, row)
postgres_conn.commit()

# Update existing records based on query2
for index, row in df2.iterrows():
    query = '''
    SELECT user_name, hosp_name, hosp_dist_id, hosp_dist_name, hosp_mandal_id, hosp_mandal_name, pswd
    FROM medical.md_user_mst
    WHERE login_name = %s
    '''
    cursor.execute(query, (row['LOGIN_ID'],))
    existing_row = cursor.fetchone()
    if existing_row:
        existing_row = dict(zip([desc[0] for desc in cursor.description], existing_row))
        if (
            existing_row['hosp_name'] != row['HOSP_NAME'] or
            existing_row['hosp_dist_id'] != row['HOSP_DIST_ID'] or
            existing_row['hosp_dist_name'] != row['HOSP_DIST_NAME'] or
            existing_row['hosp_mandal_id'] != row['HOSP_MANDAL_ID'] or
            existing_row['hosp_mandal_name'] != row['HOSP_MANDAL'] or
            existing_row['pswd'] != row['ENCRIPTED_PASSWORD']
        ):
            update_existing_login(cursor, row)
postgres_conn.commit()

# Update hosp_id based on query3
for index, row in df3.iterrows():
    query = "SELECT hosp_id FROM medical.md_user_mst WHERE login_name = %s"
    cursor.execute(query, (row['LOGIN_ID'],))
    existing_hosp_id = cursor.fetchone()
    if existing_hosp_id and existing_hosp_id[0] != row['HOSP_ID']:
        query = "UPDATE medical.md_user_mst SET hosp_id = %s WHERE login_name = %s"
        cursor.execute(query, (row['HOSP_ID'], row['LOGIN_ID']))
postgres_conn.commit()

# Insert into medical.md_user_role_mapping
for index, row in df1.iterrows():
    query = "SELECT user_id FROM medical.md_user_mst WHERE login_name = %s"
    cursor.execute(query, (row['LOGIN_ID'],))
    user_id = cursor.fetchone()
    if user_id:
        query = "INSERT INTO medical.md_user_role_mapping (user_id, role_id) VALUES (%s, %s)"
        cursor.execute(query, (user_id[0], 1))
postgres_conn.commit()

# Close PostgreSQL cursor and connection
cursor.close()
postgres_conn.close()

print("All operations completed successfully")
