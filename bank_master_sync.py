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
        user = "ysr_user_manager"
        password = "ysr_user_manager_@231129"
        host = "10.10.223.185"
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
query = '''SELECT BANK_ID, BANK_NAME , BANK_BRANCH, IFC_CODE FROM ASRIM_BANK_MASTER 
WHERE ACTIVE_YN = 'Y' FETCH NEXT 200 ROWS ONLY'''
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
print(df)

# Close Oracle cursor
cursor.close()

print("Before creating PostgreSQL connection")
# PostgreSQL Connection
postgres_conn = create_postgres_connection()
print("After creating PostgreSQL connection")

# # Function to check if bank_id e    
# Function to check if bank_id exists in ysr.tmp_bank_mst
def check_bank_id_exists(conn, bank_id):
    try:
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM ysr.ysr_bank_mst WHERE bank_id = %s"
        cursor.execute(query, (bank_id,))
        count = cursor.fetchone()[0]
        return count > 0  # Return True if bank_id exists, False otherwise
    except psycopg2.Error as e:
        print("Error checking bank_id existence:", e)
        return False  # Return False in case of an error

# Separate DataFrames for existing and non-existing banks
existing_banks = []
non_existing_banks = []
update_banks = []

# Iterate through the DataFrame and append values from the bank_id to the list to take only the existing bank
for index, row in df.iterrows():
    existing_banks.append(row['BANK_ID'])
# Print the list
print(existing_banks)

# Iterate through the DataFrame and check if bank_id already exists
for index, row in df.iterrows():
    bank_id = row['BANK_ID']
    bank_name = row['BANK_NAME']
    branch_name = row['BANK_BRANCH']
    ifsc_code = row['IFC_CODE']

    if check_bank_id_exists(postgres_conn, bank_id):
        # Fetch existing bank details from PostgreSQL
        cursor = postgres_conn.cursor()
        query = "SELECT bank_name,CASE WHEN is_active = true THEN 'Yes' ELSE 'No' END AS active_yn FROM ysr.ysr_bank_mst WHERE bank_id = %s"
        cursor.execute(query, (bank_id,))
        existing_bank_details = cursor.fetchone()
        
        query = "SELECT branch_name, ifsc_code, CASE WHEN is_active = true THEN 'Yes' ELSE 'No' END AS active_yn FROM ysr.ysr_branch_mst WHERE bank_id = %s"
        cursor.execute(query, (bank_id,))
        existing_branch_details = cursor.fetchone()

        # Convert is_active values from string to boolean
        # existing_bank_is_active = existing_bank_details[1]
        # existing_branch_is_active = existing_branch_details[2]
        if existing_bank_details is not None and existing_branch_details is not None:
            if existing_bank_details[0] != bank_name or existing_bank_details[1] != 'Yes' or existing_branch_details[0] != branch_name or existing_branch_details[1] != ifsc_code or existing_branch_details[2] != 'Yes':
                update_banks.append((bank_id, bank_name, branch_name, ifsc_code))
                print(f"Bank ID {bank_id} requires update")
    else:
        non_existing_banks.append((bank_id, bank_name, branch_name, ifsc_code))
        print(f"Bank ID {bank_id} doesn't exist in the database")

def update_inactive_banks_flag(postgres_conn, existing_bank_ids):
    try:
        cursor = postgres_conn.cursor()
        
        # Fetch all bank IDs from both ysr.tmp_bank_mst and ysr.tmp_branch_mst
        cursor.execute("SELECT bank_id FROM ysr.ysr_bank_mst WHERE is_active = true UNION SELECT bank_id FROM ysr.ysr_branch_mst WHERE is_active = true")
        all_bank_ids = [row[0] for row in cursor.fetchall()]
        existing_bank_ids = [int(bank_id) for bank_id in existing_bank_ids]
        # Print the bank IDs fetched
        print("ALL BANK IDS:", all_bank_ids)
        print("EXISTING BANK IDS:", existing_bank_ids)

        # Find bank IDs present in all_bank_ids but not in existing_bank_ids
        inactive_bank_ids = [x for x in all_bank_ids if x not in existing_bank_ids]
        print("inactive:",inactive_bank_ids)

        # If there are no inactive bank IDs, return without updating
        if not inactive_bank_ids:
            print("No inactive banks found, skipping update")
            return

        # Update is_active flag for inactive bank IDs
        for bank_id in inactive_bank_ids:
            sql_update_is_inactive = "UPDATE ysr.ysr_bank_mst SET is_active = false WHERE bank_id = %s"
            cursor.execute(sql_update_is_inactive, (bank_id,))

            sql_update_is_inactive_branch = "UPDATE ysr.ysr_branch_mst SET is_active = false WHERE bank_id = %s"
            cursor.execute(sql_update_is_inactive_branch, (bank_id,))

        postgres_conn.commit()
        print("Inactive banks updated successfully")
    except psycopg2.Error as e:
        postgres_conn.rollback()
        print("Error updating inactive banks:", e)
    finally:
        cursor.close()


# Function to execute insert queries for non-existing banks
def execute_postgres_insert_queries(conn, insert_queries):
    try:
        cursor = conn.cursor()
        for bank_id, bank_name, branch_name, ifsc_code in insert_queries:
            sql_insert_bank = "INSERT INTO ysr.ysr_bank_mst (bank_id, bank_name) VALUES (%s, %s)"
            cursor.execute(sql_insert_bank, (bank_id, bank_name))
            sql_insert_branch = "INSERT INTO ysr.ysr_branch_mst (branch_name, ifsc_code, bank_id) VALUES (%s, %s, %s)"
            cursor.execute(sql_insert_branch, (branch_name, ifsc_code, bank_id))
        conn.commit()
        print("Data inserted successfully")
    except psycopg2.Error as e:
        conn.rollback()
        print("Error inserting data:", e)
    finally:
        cursor.close()


# Function to execute update queries for non-existing banks
def execute_postgres_update_queries(conn, insert_queries):
    try:
        cursor = conn.cursor()
        for bank_id, bank_name, branch_name, ifsc_code in insert_queries:
            sql_update_bank = "UPDATE ysr.ysr_bank_mst SET bank_name = %s, is_active = true WHERE bank_id = %s"
            cursor.execute(sql_update_bank, (bank_name, bank_id))
            sql_update_branch = "UPDATE ysr.ysr_branch_mst SET branch_name = %s, ifsc_code = %s, is_active = true WHERE bank_id = %s"
            cursor.execute(sql_update_branch, (branch_name, ifsc_code, bank_id))
        conn.commit()
        print("Data inserted successfully")
    except psycopg2.Error as e:
        conn.rollback()
        print("Error inserting data:", e)
    finally:
        cursor.close()


try:
    # After executing insert queries for non-existing banks
    execute_postgres_insert_queries(postgres_conn, non_existing_banks)

    # After executing update queries for existing banks
    execute_postgres_update_queries(postgres_conn, update_banks)

    # Call the function to update inactive banks flag
    update_inactive_banks_flag(postgres_conn, existing_banks)

    # Set the status to success if all operations were successful
    status = "success"
except Exception as ex:
    # If any exception occurred during the execution, set status to failure
    status = "failure"
    print("An unexpected error occurred:", ex)
    # You may include the error message in the status insertion
    error_message = str(ex)

def insert_scheduler_status(conn, scheduler_name, status, execution_time=None, error_msg=None):
    try:
        cursor = conn.cursor()
        if execution_time is None:
            execution_time = datetime.datetime.now()
        cursor.execute(
            "INSERT INTO ysr.ysr_scheduler_status (scheduler_name, status, execution_time, error_msg) "
            "VALUES (%s, %s, %s, %s)",
            (scheduler_name, status, execution_time, error_msg)
        )
        conn.commit()
        print("Scheduler status inserted successfully")
    except psycopg2.Error as e:
        conn.rollback()
        print("Error inserting scheduler status:", e)
    finally:
        cursor.close()

# Insert scheduler status after all operations are done
insert_scheduler_status(postgres_conn, "BANK MASTER SYNC", status, error_msg=error_message if status == "failure" else None)
