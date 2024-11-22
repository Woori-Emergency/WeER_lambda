import os
import pymysql
import requests
from xml.etree import ElementTree as ET
import time
from datetime import datetime, timedelta

# 환경변수
ANNOUNCEMENT_API_URL = os.getenv('ANNOUNCEMENT_API_URL') # "http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEmrrmSrsillDissMsgInqire"
API_URL = os.getenv('REALTIME_API_URL')  # "http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEmrrmRltmUsefulSckbdInfoInqire"
SERVICE_KEY = os.getenv('OPENAPI_SERVICE_KEY')
RDS_HOST = os.getenv('RDS_HOST')
RDS_PORT = int(os.getenv('RDS_PORT'))
RDS_USER = os.getenv('RDS_USER')
RDS_PASSWORD = os.getenv('RDS_PASSWORD')
RDS_DB = os.getenv('RDS_DB')

DISTRICTS = [
    "강남구", "강동구", "강북구", "강서구", "관악구", "광진구",
    "구로구", "금천구", "노원구", "도봉구", "동대문구", "동작구", "마포구",
    "서대문구", "서초구", "성동구", "성북구", "송파구", "양천구", "영등포구",
    "용산구", "은평구", "종로구", "중구", "중랑구"
]

####### 장비, 응급실, 중환자실 #######

def fetch_api_data(stage1, stage2, pageNo=1, numOfRows=10):
    query_params = {
        "serviceKey": SERVICE_KEY,
        "STAGE1": stage1,
        "STAGE2": stage2,
        "pageNo": str(pageNo),
        "numOfRows": str(numOfRows)
    }

    response = requests.get(API_URL, params=query_params)
    response.raise_for_status()
    return response.text

def safe_findtext(element, tag):
    """Helper function to safely get text from an XML tag."""
    try:
        return element.findtext(tag)
    except AttributeError:
        print(f"Warning: '{tag}' field is missing in the XML response.")
        return None

def parse_boolean(value):
    """Convert 'Y' to True and 'N1' to False; otherwise, return None."""
    if value == "Y":
        return True
    elif value == "N1":
        return False
    return None

def parse_xml_to_dict(xml_data, fields, boolean_fields=None):
    """Parse XML data into a list of dictionaries."""
    root = ET.fromstring(xml_data)
    result_code = root.findtext(".//resultCode")
    if result_code != "00":
        raise ValueError(f"API Error: {root.findtext('.//resultMsg')}")
    items = root.findall(".//item")
    parsed_data = []
    for item in items:
        parsed_item = {}
        for field in fields:
            parsed_item[field] = safe_findtext(item, field)

        if boolean_fields:
            for boolean_field in boolean_fields:
                parsed_item[boolean_field] = parse_boolean(safe_findtext(item, boolean_field))
        parsed_data.append(parsed_item)
    return parsed_data

def upsert_data(data_list, table_name, fields, unique_field="hpid"):
    """RDS 테이블에 데이터를 upsert (insert or update)한다."""
    print(f"[DEBUG] RDS 연결 정보: Host={RDS_HOST}, Port={RDS_PORT}, DB={RDS_DB}")
    conn = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        db=RDS_DB,
        port=RDS_PORT,
        charset="utf8mb4"
    )
    try:
        with conn.cursor() as cursor:
            print(f"[INFO] '{table_name}' 테이블에 데이터 upsert 시작.")
            for data in data_list:
                if not isinstance(data, dict):
                    print(f"[ERROR] 잘못된 데이터 형식: {data}")
                    continue

                # 현재 한국 시간 가져오기
                now_kst = get_current_kst()

                # 존재 여부 확인 쿼리
                select_query = f"SELECT COUNT(*) FROM {table_name} WHERE {unique_field} = %s"
                cursor.execute(select_query, (data[unique_field],))
                exists = cursor.fetchone()[0] > 0

                if exists:
                    # UPDATE 쿼리 생성
                    update_fields = ", ".join([f"{field} = %s" for field in fields if field != unique_field])
                    update_query = f"""
                        UPDATE {table_name} 
                        SET {update_fields}, modified_at = %s 
                        WHERE {unique_field} = %s
                    """
                    update_values = [data.get(field) for field in fields if field != unique_field] + [now_kst, data[unique_field]]
                    cursor.execute(update_query, update_values)
                    print(f"[INFO] '{data[unique_field]}' 데이터가 업데이트되었습니다.")
                else:
                    # INSERT 쿼리 생성
                    placeholders = ", ".join(["%s"] * (len(fields) + 2))  # created_at과 modified_at 포함
                    columns = ", ".join(fields + ["created_at", "modified_at"])
                    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    insert_values = [data.get(field) for field in fields] + [now_kst, now_kst]
                    cursor.execute(insert_query, insert_values)
                    print(f"[INFO] '{data[unique_field]}' 데이터가 삽입되었습니다.")
            
            conn.commit()
            print(f"[INFO] '{table_name}' 테이블에 데이터 저장 성공.")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] RDS 저장 중 오류 발생: {e}")
        raise
    finally:
        conn.close()
        print("[INFO] RDS 연결이 종료되었습니다.")

def get_current_kst():
    """현재 한국 시간을 반환합니다."""
    now_utc = datetime.utcnow()
    now_kst = now_utc + timedelta(hours=9)  # UTC + 9 hours
    return now_kst

def lambda_realtime():
    stage1 = "서울특별시"
    for district in DISTRICTS:
        try:
            xml_response = fetch_api_data(stage1, district)
            
            # Equipment Data
            equipment_data = parse_xml_to_dict(
                xml_response,
                fields=["hpid", "hvs30", "hvs31", "hvs32", "hvs33", "hvs34", "hvs35", "hvs37", "hvs27", "hvs28", "hvs29"],
                boolean_fields=["hvventiayn", "hvventisoayn", "hvincuayn", "hvcrrtayn", "hvecmoayn", "hvhypoayn", "hvoxyayn", "hvctayn", "hvmriayn", "hvangioayn"]
            )
            upsert_data(equipment_data, "equipment", ["hpid", "hvventiayn", "hvventisoayn", "hvincuayn", "hvcrrtayn", "hvecmoayn", "hvhypoayn", "hvoxyayn", "hvctayn", "hvmriayn", "hvangioayn", "hvs30", "hvs31", "hvs32", "hvs33", "hvs34", "hvs35", "hvs37", "hvs27", "hvs28", "hvs29"])

            # ICU Data
            icu_data = parse_xml_to_dict(
                xml_response,
                fields=["hpid", "hvcc", "hvncc", "hvccc", "hvicc", "hv2", "hv3", "hv6", "hv8", "hv9", "hv32", "hv34", "hv35", "hvs11", "hvs08", "hvs16", "hvs17", "hvs06", "hvs07", "hvs12", "hvs13", "hvs14", "hvs09", "hvs15", "hvs18"]
            )
            upsert_data(icu_data, "icu", ["hpid", "hvcc", "hvncc", "hvccc", "hvicc", "hv2", "hv3", "hv6", "hv8", "hv9", "hv32", "hv34", "hv35", "hvs11", "hvs08", "hvs16", "hvs17", "hvs06", "hvs07", "hvs12", "hvs13", "hvs14", "hvs09", "hvs15", "hvs18"])

            # Emergency Data
            emergency_data = parse_xml_to_dict(
                xml_response,
                fields=["hpid", "hvec", "hv27", "hv29", "hv30", "hv28", "hv15", "hv16", "hvs01", "hvs59", "hvs03", "hvs04", "hvs02", "hvs48", "hvs49"]
            )
            upsert_data(emergency_data, "emergency", ["hpid", "hvec", "hv27", "hv29", "hv30", "hv28", "hv15", "hv16", "hvs01", "hvs59", "hvs03", "hvs04", "hvs02", "hvs48", "hvs49"])

        except Exception as e:
            print(f"Error occurred for district {district}: {e}")


####### 공지사항 #######

def fetch_announce_data(stage1, stage2, pageNo=1, numOfRows=10):
    query_params = {
        "serviceKey": SERVICE_KEY,
        "Q0": stage1,
        "Q1": stage2,
        "pageNo": str(pageNo),
        "numOfRows": str(numOfRows)
    }
    response = requests.get(ANNOUNCEMENT_API_URL, params=query_params)
    response.raise_for_status()
    return response.text

def parse_announcement_xml_to_dict(xml_data, fields, datetime_fields=None):
    """Parse XML data into a list of dictionaries."""
    root = ET.fromstring(xml_data)
    result_code = root.findtext(".//resultCode")
    if result_code != "00":
        raise ValueError(f"API Error: {root.findtext('.//resultMsg')}")

    items = root.findall(".//item")
    parsed_data = []
    for item in items:
        parsed_item = {field: safe_findtext(item, field) for field in fields}
        if datetime_fields:
            for datetime_field in datetime_fields:
                raw_value = safe_findtext(item, datetime_field)
                parsed_item[datetime_field] = (
                    datetime.strptime(raw_value, "%Y%m%d%H%M%S") if raw_value else None
                )
        parsed_data.append(parsed_item)
    return parsed_data

def get_hospital_id(hpid):
    """Retrieve hospital ID from the hospital table using the hpid."""
    conn = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        db=RDS_DB,
        port=RDS_PORT
    )
    hospital_id = None
    with conn.cursor() as cursor:
        cursor.execute("SELECT hospital_id FROM hospital WHERE hpid = %s", (hpid,))
        result = cursor.fetchone()
        if result:
            hospital_id = result[0]
    conn.close()
    return hospital_id

def clear_and_store_er_announcement(data_list):
    """
    Delete existing ER Announcement data and insert new data.
    
    Args:
        data_list (list): List of ER Announcement data dictionaries.
    """
    conn = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        db=RDS_DB,
        port=RDS_PORT
    )
    try:
        with conn.cursor() as cursor:
            # 기존 데이터를 삭제
            cursor.execute("DELETE FROM er_announcement")
            print("Deleted all existing data from er_announcement.")

            # 새로운 데이터 삽입
            for data in data_list:
                hospital_id = get_hospital_id(data["hpid"])
                if not hospital_id:
                    print(f"Hospital not found for hpid: {data['hpid']}")
                    continue

                sql = """
                    INSERT INTO er_announcement (hospital_id, msg_type, message, disease_type, start_time, end_time, created_at, modified_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    hospital_id,
                    data.get("symBlkMsgTyp"),
                    data.get("symBlkMsg"),
                    data.get("symTypCod"),
                    data.get("symBlkSttDtm"),
                    data.get("symBlkEndDtm"),
                    datetime.now(),
                    datetime.now()
                ))
                print(f"Inserted ER_Announcement for hospital_id: {hospital_id}")

            # 트랜잭션 커밋
            conn.commit()
            print("Transaction committed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error occurred: {e}")
    finally:
        conn.close()
        print("Connection closed.")

def lambda_announcement():
    stage1 = "서울특별시"
    all_data = []

    for district in DISTRICTS:
        try:
            announce_xml_response = fetch_announce_data(stage1, district)
            # Skip if no items in response
            if "<items/>" in announce_xml_response:
                print(f"No data available for district {district}.")
                continue

            er_announcement_data = parse_announcement_xml_to_dict(
                announce_xml_response,
                fields=["hpid", "symBlkMsgTyp", "symBlkMsg", "symTypCod"],
                datetime_fields=["symBlkSttDtm", "symBlkEndDtm"]
            )
            all_data.extend(er_announcement_data)

        except Exception as e:
            print(f"Error occurred for district {district}: {e}")

    # Now call the function to clear and store data
    if all_data:
        clear_and_store_er_announcement(all_data)
    else:
        print("No data to store in ER Announcement.")




def lambda_handler(event=None, context=None):
    start_time = time.time()

    lambda_realtime() #장비, 응급실, 중환자실 
    lambda_announcement() #공지사항

    end_time = time.time()
    print(f"Total time taken: {end_time - start_time} seconds")
    return {
        "statusCode": 200,
        "body": "Data fetched and stored successfully for all districts"
    }

