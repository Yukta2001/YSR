import requests
import pandas as pd
import oracledb
import json


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

#connect oracle and fetch data from table
def read_data_fromr_db():
    try:
        cursor = get_oracle_connection()
        query = '''SELECT RESIDENT_ID,UID_HASH_VALUE FROM asrit_family_cs_ap
        UNION ALL
        SELECT RESIDENT_ID,UID_HASH_VALUE FROM asrit_tap_family_ap
        UNION ALL
        SELECT RESIDENT_ID,UID_HASH_VALUE FROM asrit_janmabhoomi_family
        UNION ALL
        SELECT RESIDENT_ID,UID_HASH_VALUE FROM asrit_family_cs_amaravati
        FETCH NEXT 10 ROWS ONLY'''
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        print(df)
        aadhar_numbers = [row[1] for row in df]
        cursor.close()
        return aadhar_numbers
    except Exception as e:
        print(f"Erroe: {e}")
        return None


def decrypt_aadhar(aadhar_numbers):
    try:
        api_url = "http://push161.sps.ap.gov.in/ThirdParty/api/Service/aarogyasree_uid_details"
        headers = {"Content-Type":"application/json","username":"AAROGYA_SREE","password":"AAROGYA_SREE@4682"}
        dta = {"aadhaar_number" : aadhar_numbers}
        response = requests.post(api_url,data=json.dumps(dta) ,headers=headers)
        if response.status_code == 200:
            encrypt_aadhar = response.json().get('decrypt_aadhar')
            return encrypt_aadhar
        else:
            print(f"Aadhar encryption failed: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    
aadhar_num = read_data_fromr_db()
if aadhar_num:
    encrypted_aadhar = decrypt_aadhar(aadhar_num)
    if encrypted_aadhar:
        print("Decrypted aadhar: ",encrypted_aadhar)
    else:
         print("Decryption Failed")
else:
     print("Aadhar not found in database")