import requests
import pandas as pd
import time
import os
from datetime import datetime

CSV_FILE = "air_quality_weather_data.csv"

AQI_IDS = ["31390903576425084107499649578", "28560877461938780203765592307", "31390908889087377344742439468", 
           "31390912357075263208060500522", "31390916083317566102523755051", "31388851800421997746903202346",
           "29195707587706641566224751462", "28505268571336961948594948504", "31387251434693138681789561386",
           "31388883344354363840031242796", "31390932574706768021562473002", "31388839920718814259329251882",
           "29213751141295132066317063859"]
WEATHER_IDS = ["1581130", "1561096", "1581364", "1566083", "1566083", "1594018",
               "1566319", "1562820", "1591527", "1570449", "1567069", "1565022"]  

def fetch_air_quality(id):
    url_aqi = f"https://envisoft.gov.vn/eos/services/call/json/qi_detail_for_eip?station_id={id}"
    response = requests.get(url_aqi)
    if response.status_code != 200:
        print(f"Lỗi khi lấy dữ liệu AQI cho ID {id}")
        return None

    data = response.json()

    try:
        name = data['station_name']
        time = data['qi_time_2']
        aqi = data['qi_value']
        res = data.get('res', {})
        pm10 = res.get('PM-10', {}).get('current', None)
        pm25 = res.get('PM-2-5', {}).get('current', None)
        co = res.get('CO', {}).get('current', None) 
        no2 = res.get('NO2', {}).get('current', None)
        o3 = res.get('O3', {}).get('current', None)
        so2 = res.get('SO2', {}).get('current', None)
        return {
            "Timestamp": time,
            "Name": name,
            "AQI": aqi,
            "PM2.5": pm25,
            "PM10": pm10,
            "CO": co,
            "NO2": no2,
            "O3": o3,
            "SO2": so2,
        }
    except Exception as e:
        print("Lỗi khi xử lý dữ liệu AQI:", e)
        return None

def fetch_weather(id):
    url_weather = f"https://api.openweathermap.org/data/2.5/weather?id={id}&appid=b3c6503ac31cb5e72765e98cd0d7795e&units=metric&lang=vi"
    response = requests.get(url_weather)
    if response.status_code != 200:
        print(f"Lỗi khi lấy dữ liệu thời tiết cho ID {id}")
        return None

    data = response.json()

    try:
        lat = data['coord']['lat']
        lon = data['coord']['lon']
        temp = data['main']['temp']
        pressure = data['main']['pressure']
        humidity = data['main']['humidity']
        wind_speed = data['wind']['speed']
        return {
            "Latitude": lat,
            "Longitude": lon,
            "Temperature": temp,
            "Humidity": humidity,
            "Pressure": pressure,
            "Wind Speed": wind_speed
        }
    except Exception as e:
        print("Lỗi khi xử lý dữ liệu thời tiết:", e)
        return None



def read_csv_data():
    if os.path.exists(CSV_FILE):
        df = pd.read_csv(CSV_FILE)
        # Đảm bảo các cột đúng định dạng (tránh lỗi thiếu cột do file cũ)
        expected_columns = [
            "Timestamp", "Name", "Latitude", "Longitude",
            "AQI", "PM2.5", "PM10", "CO", "NO2", "O3", "SO2",
            "Temperature", "Humidity", "Pressure", "Wind Speed"
        ]
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        df = df[expected_columns]  # Đảm bảo đúng thứ tự cột
        return df

    # Nếu chưa có file thì tạo dataframe với cấu trúc chuẩn
    return pd.DataFrame(columns=[
        "Timestamp", "Name", "Latitude", "Longitude",
        "AQI", "PM2.5", "PM10", "CO", "NO2", "O3", "SO2",
        "Temperature", "Humidity", "Pressure", "Wind Speed"
    ])

def save_to_csv(data):
    df = read_csv_data()

    # Tạo dòng mới đúng thứ tự cột để tránh FutureWarning
    new_row = pd.DataFrame([{col: data.get(col, None) for col in df.columns}])

    # Kiểm tra trùng lặp dựa trên Name + Timestamp
    mask = (df["Name"] == data["Name"]) & (df["Timestamp"] == data["Timestamp"])

    if mask.any():
        df = df[~mask]
        print(f"Đã cập nhật dữ liệu cho {data['Name']} vào {data['Timestamp']}.")
    else:
        print(f"Đã thêm dữ liệu mới cho {data['Name']} vào {data['Timestamp']}.")

    # Thêm dòng mới (đã đúng cấu trúc)
    df = pd.concat([df, new_row], ignore_index=True)

    # Ghi ra file
    df.to_csv(CSV_FILE, index=False, encoding="utf-8")

def crawl_all_data():
    df_existing = read_csv_data()
    max_len = max(len(AQI_IDS), len(WEATHER_IDS))

    for i in range(max_len):
        aqi_id = AQI_IDS[i]
        weather_id = WEATHER_IDS[i] if i < len(WEATHER_IDS) else None

        # Tạm thời gọi fetch AQI trước, để lấy được Name và Timestamp để kiểm tra
        aqi_data = fetch_air_quality(aqi_id)
        if not aqi_data:
            print("Bỏ qua do không có dữ liệu AQI.")
            continue

        name = aqi_data["Name"]
        timestamp = aqi_data["Timestamp"]

        # Kiểm tra xem dữ liệu đã tồn tại chưa
        mask = (df_existing["Name"] == name) & (df_existing["Timestamp"] == timestamp)
        if mask.any():
            print(f" Đã có dữ liệu của {name} vào {timestamp}, bỏ qua crawl.")
            continue

        print(f"\n Đang lấy dữ liệu cho trạm {name}...")

        weather_data = fetch_weather(weather_id) if weather_id else {}
        if not weather_data:
            print("Không có dữ liệu thời tiết, dùng giá trị rỗng.")
            weather_data = {
                "Latitude": None,
                "Longitude": None,
                "Temperature": None,
                "Humidity": None,
                "Pressure": None,
                "Wind Speed": None
            }

        combined_data = {**aqi_data, **weather_data}
        save_to_csv(combined_data)

        # Cập nhật df_existing luôn để tránh trùng trong cùng lần chạy
        df_existing = pd.concat([df_existing, pd.DataFrame([combined_data])], ignore_index=True)

        time.sleep(3)


if __name__ == "__main__":
    crawl_all_data()
