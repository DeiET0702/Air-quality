import ee
import datetime
import csv
import os
import json
import logging
import pytz

# Thiết lập logging
logging.basicConfig(filename='sentinel_update.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Khởi tạo Earth Engine với service account
try:
    gee_key = json.loads(os.getenv('GEE_SERVICE_ACCOUNT_KEY', '{}'))
    if not gee_key:
        raise ValueError("Không tìm thấy GEE_SERVICE_ACCOUNT_KEY")
    credentials = ee.ServiceAccountCredentials(gee_key['client_email'], gee_key)
    ee.Initialize(credentials)
    logging.info("Khởi tạo Earth Engine thành công")
except Exception as e:
    logging.error(f"Lỗi khởi tạo Earth Engine: {e}")
    raise

# Danh sách tọa độ
locations = [
    ("Hà Nội: ĐHBK cổng Parabol đường Giải Phóng", 21.0052, 105.8418),
    ("Hà Nội: 556 Nguyễn Văn Cừ", 21.0491, 105.8831),
    ("Hà Nội: Công viên Nhân Chính - Khuất Duy Tiến", 21.0031, 105.7947),
    ("HCM: Khu Liên cơ quan Bộ Tài Nguyên và Môi Trường - số 20 Đ. Lý Chính Thắng", 10.7823, 106.6834),
    ("HCM: Đ. Lê Hữu Kiều - P. Bình Trưng Tây - Quận 2 (Ngã ba Lê Hữu Kiểu và Trương Văn Bang)", 10.7823, 106.7528),
    ("Đà Nẵng: Khuôn viên trường ĐH sư phạm Đà Nẵng", 16.0622, 108.1594),
    ("Thái nguyên: Đường Hùng Vương - Tp Thái Nguyên", 21.59315, 105.8431),
    ("Phú Thọ: đường Hùng Vương - Tp Việt Trì", 21.33847, 105.3633),
    ("Bắc Giang: Khu liên cơ quan tỉnh Bắc Giang - P. Ngô Quyền - TP. Bắc Giang", 21.3015, 106.22603),
    ("Hà Nam: Công Viên Nam Cao - P.Quang Trung - TP. Phủ Lý", 20.536, 105.9165),
    ("Long An: UBND Tp Tân An - 76 Hùng Vương - P.2", 10.5391, 106.4045),
    ("Bình Dương: số 593 Đại lộ Bình Dương, P. Hiệp Thành", 10.9923, 106.6577),
    ("Quảng Bình: Khu kinh tế Hòn La", 17.9329, 106.4966)
]

# Các chỉ số Sentinel-5P (dùng NRTI)
s5p_variables = {
    'NO2': ['COPERNICUS/S5P/NRTI/L3_NO2', 'tropospheric_NO2_column_number_density'],
    'SO2': ['COPERNICUS/S5P/NRTI/L3_SO2', 'SO2_column_number_density'],
    'CO': ['COPERNICUS/S5P/NRTI/L3_CO', 'CO_column_number_density'],
    'O3': ['COPERNICUS/S5P/NRTI/L3_O3', 'O3_column_number_density'],
    'CH4': ['COPERNICUS/S5P/NRTI/L3_CH4', 'CH4_column_volume_mixing_ratio_dry_air'],
    'HCHO': ['COPERNICUS/S5P/NRTI/L3_HCHO', 'tropospheric_HCHO_column_number_density'],
    'AER_AI': ['COPERNICUS/S5P/NRTI/L3_AER_AI', 'absorbing_aerosol_index'],
    'AER_LH': ['COPERNICUS/S5P/NRTI/L3_AER_LH', 'aerosol_height'],
    'CLOUD': ['COPERNICUS/S5P/NRTI/L3_CLOUD', 'cloud_fraction']
}

def get_daily_avg(date_str, location_name, lat, lon):
    point = ee.Geometry.Point([lon, lat])
    region = point.buffer(1000).bounds()  # Vùng đệm 10km

    date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    start_date = ee.Date(date.strftime("%Y-%m-%d"))
    end_date = ee.Date((date + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))

    result = {"location": location_name, "date": date_str}
    logging.info(f"Lấy dữ liệu cho {location_name} ngày {date_str}")

    for var, (dataset, band) in s5p_variables.items():
        try:
            collection = ee.ImageCollection(dataset).filterDate(start_date, end_date)
            if collection.size().getInfo() == 0:
                logging.warning(f"Không có dữ liệu {var} tại {location_name} ngày {date_str}")
                result[var] = None
                continue
            image = collection.mean()
            mean_dict = image.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=region,
                scale=1000,
                maxPixels=1e9
            )
            value = mean_dict.get(band)
            result[var] = value.getInfo() if value else None
        except Exception as e:
            logging.error(f"Lỗi khi lấy {var} tại {location_name} ngày {date_str}: {e}")
            result[var] = None

    return result

# Tạo danh sách dữ liệu đã có
existing_records = set()
csv_file = "sentinel5P_NRTI_data.csv"
if os.path.exists(csv_file):
    try:
        with open(csv_file, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_records.add((row["date"], row["location"]))
        logging.info(f"Tìm thấy {len(existing_records)} bản ghi hiện có")
    except Exception as e:
        logging.error(f"Lỗi khi đọc CSV: {e}")
        raise

# Ghi thêm dữ liệu mới (chỉ cho ngày hôm qua)
fieldnames = ["date", "location"] + list(s5p_variables.keys())
vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')
today = datetime.datetime.now(vietnam_tz).date()
start_date = today - datetime.timedelta(days=1)  # Ngày hôm qua
date_str = start_date.strftime("%Y-%m-%d")

with open(csv_file, "a", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    if os.stat(csv_file).st_size == 0:
        writer.writeheader()
        logging.info("Tạo file CSV mới với header")

    for name, lat, lon in locations:
        if (date_str, name) in existing_records:
            logging.info(f"Bỏ qua {date_str} - {name}: Đã tồn tại")
            continue
        try:
            data = get_daily_avg(date_str, name, lat, lon)
            writer.writerow(data)
            logging.info(f"✔ Đã lưu: {date_str} - {name}")
        except Exception as e:
            logging.error(f"⚠ Lỗi tại {date_str} - {name}: {e}")