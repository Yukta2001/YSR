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

query3 = '''SELECT login_name, hosp_id FROM asrim_nwh_users 
WHERE eff_end_dt is null 
AND crt_dt >= trunc(sysdate) - 1'''

cursor.execute(query1)
df1 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

cursor.execute(query2)
df2 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

cursor.execute(query3)
df3 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

# Close Oracle cursor and connection
cursor.close()
connection.close()

print("Before creating PostgreSQL connection")
# PostgreSQL Connection
postgres_conn = create_postgres_connection()
print("After creating PostgreSQL connection")

# Fetch existing logins from medical.md_user_mst
existing_logins = {}
with postgres_conn.cursor() as cursor:
    cursor.execute("SELECT login_name, hosp_id FROM medical.md_user_mst")
    existing_logins = {row[0]: row[1] for row in cursor.fetchall()}

# Function to insert new login_id details
def insert_new_login(cursor, row):
    query = '''
    INSERT INTO medical.md_user_mst (user_name, login_name, pswd, hospital_name, hosp_id, hosp_dist_id, hosp_dist_name, hosp_mandal_id, hosp_mandal_name, is_active)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    cursor.execute(query, (
        row['USER_NAME'], row['LOGIN_ID'], row['ENCRIPTED_PASSWORD'], row['HOSP_NAME'],
        row['HOSP_ID'], row['HOSP_DIST_ID'], row['HOSP_DIST_NAME'], row['HOSP_MANDAL_ID'],
        row['HOSPITAL_MANDAL'], True if row['ACTIVE_YN'] == 'Y' else False
    ))

# Function to update existing login_id details
def update_existing_login(cursor, row):
    query = '''
    UPDATE medical.md_user_mst
    SET user_name = %s, hosp_name = %s, hosp_dist_id = %s, hosp_dist_name = %s,
        hosp_mandal_id = %s, hosp_mandal_name = %s, pswd = %s, is_active = %s
    WHERE login_name = %s
    '''
    cursor.execute(query, (
        row['USER_NAME'], row['HOSP_NAME'], row['HOSP_DIST_ID'], row['HOSP_DIST_NAME'],
        row['HOSP_MANDAL_ID'], row['HOSPITAL_MANDAL'], row['ENCRIPTED_PASSWORD'],
        True if row['ACTIVE_YN'] == 'Y' else False, row['LOGIN_ID']
    ))

# Function to update hospital ID for existing login_id
def update_hospital_id(cursor, login_id, new_hosp_id):
    query = '''
    UPDATE medical.md_user_mst
    SET hosp_id = %s
    WHERE login_name = %s
    '''
    cursor.execute(query, (new_hosp_id, login_id))

# Function to insert into medical.md_user_role_mapping
def insert_user_role_mapping(cursor, login_id):
    query = '''
    INSERT INTO medical.md_user_role_mapping (user_id, role_id)
    SELECT user_id, 1 FROM medical.md_user_mst WHERE login_name = %s
    '''
    cursor.execute(query, (login_id,))

# Insert records from query1
with postgres_conn.cursor() as cursor:
    for index, row in df1.iterrows():
        if row['LOGIN_ID'] not in existing_logins:
            insert_new_login(cursor, row)
            insert_user_role_mapping(cursor, row['LOGIN_ID'])
    postgres_conn.commit()

# Update records based on query2
with postgres_conn.cursor() as cursor:
    for index, row in df2.iterrows():
        update_existing_login(cursor, row)
    postgres_conn.commit()

# Update hospital ID if changed based on query3
with postgres_conn.cursor() as cursor:
    for index, row in df3.iterrows():
        if row['LOGIN_NAME'] in existing_logins and row['HOSP_ID'] != existing_logins[row['LOGIN_NAME']]:
            update_hospital_id(cursor, row['LOGIN_NAME'], row['HOSP_ID'])
    postgres_conn.commit()

# Close PostgreSQL connection
postgres_conn.close()

print("All operations completed successfully")
