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
    return cursor

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
cursor = get_oracle_connection()
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
print(df1)

cursor.execute(query2)
df2 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
print(df2)

cursor.execute(query3)
df3 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
print(df3)
# Close Oracle cursor
cursor.close()

print("Before creating PostgreSQL connection")
# PostgreSQL Connection
postgres_conn = create_postgres_connection()
print("After creating PostgreSQL connection")

# Iterate through the DataFrame and check if login_id already exists in the database
for index, row in df1.iterrows():
    login = row['LOGIN_ID']
    user = row['USER_NAME']
    hosp_id = row['HOSP_ID']
    hosp_name = row['HOSP_NAME']
    hosp_dist_id = row['HOSP_DIST_ID']
    hosp_dist_name = row['HOSP_DIST_NAME']
    hosp_mandal_id = row['HOSP_MANDAL_ID']
    hosp_mandal_name = row['HOSP_MANDAL_NAME']
    enc_password = row['ENCRIPTED_PASSWORD']

# Function to execute insert queries for non-existing logins
def execute_postgres_insert_queries(conn, insert_queries):
    try:
        cursor = conn.cursor()
        for login, user, enc_password in insert_queries:
            sql_insert_login = "INSERT INTO medical.md_user_mst (user_name,login_name,pswd,hospital_name,hosp_id,hosp_dist_id,hosp_dist_name,hosp_mandal_id,hoap_mandal_name) VALUES (%s, %s, %s, %s, %s, %s,%s,%s,%s)"
            cursor.execute(sql_insert_login, (user,login,enc_password,hosp_name,hosp_id,hosp_dist_id,hosp_dist_name,hosp_mandal_id,hosp_mandal_name))
        conn.commit()
        print("Data inserted successfully")
    except psycopg2.Error as e:
        conn.rollback()
        print("Error inserting data:", e)
    finally:
        cursor.close()

try:
    # Insert non-existing logins
    execute_postgres_insert_queries(postgres_conn)

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
