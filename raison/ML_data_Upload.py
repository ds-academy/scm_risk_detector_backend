import os
import json
import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine
from module.data.database.db_config import db_config

# ✅ DB 연결 설정
engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

# ✅ 뉴스 & 주가 데이터 폴더 경로
news_base_path = "C:/Users/smhrd/scm_risk_detector_backend/data/news"
output_path = "C:/Users/smhrd/scm_risk_detector_backend/raison/data"  # CSV 저장 폴더

# ✅ news 폴더에서 회사 코드 리스트 가져오기
company_codes = [
    folder for folder in os.listdir(news_base_path)
    if os.path.isdir(os.path.join(news_base_path, folder)) and not folder.startswith("__")
]

# ✅ 모든 회사 코드에 대해 반복 실행
for company in company_codes:

    try:
        # 🔹 주식 데이터 불러오기 (SQL Query 실행)
        query = f"""
        SELECT DATE, OPEN, HIGH, LOW, CLOSE, VOLUME
        FROM STOCK_PRICE
        WHERE COMPANY_CODE = '{company}' AND DATE >= '2025-01-01' AND DATE <= '2025-02-03'
        """
        stock_df = pd.read_sql(query, engine)
        stock_df['DATE'] = pd.to_datetime(stock_df['DATE']).dt.date  # 날짜 변환

        # 🔹 뉴스 감성 데이터 불러오기 (각 회사별 `report.json` 읽기)
        news_path = os.path.join(news_base_path, company, "report.json")
        with open(news_path, 'r', encoding='utf-8') as f:
            news_data = json.load(f)
        df = pd.DataFrame.from_dict(news_data, orient='index')

        df = df[['pubDate', 'sentiment']]  # 날짜 + 감성 점수만
        df['pubDate'] = pd.to_datetime(df['pubDate']).dt.date  # 날짜 변환
        df['sentiment'] = pd.to_numeric(df['sentiment'], errors='coerce')
        df = df.groupby('pubDate', as_index=False)['sentiment'].mean()  # 중복 sentiment값 평균처리

        # 🔹 데이터 병합 (`주식 데이터 기준`으로 병합)
        merged_data = pd.merge(df, stock_df, left_on='pubDate', right_on='DATE', how='outer')

        # 🔹 sentiment 값을 가장 가까운 날의 값으로 채우기
        merged_data.loc[:, 'DATE':'VOLUME'] = merged_data.loc[:, 'DATE':'VOLUME'].bfill()

        # 🔹 불필요한 컬럼 삭제 및 인덱스 초기화
        merged_data.drop(columns=['DATE', 'VOLUME'], inplace=True)
        merged_data.dropna(inplace=True)
        merged_data.reset_index(drop=True, inplace=True)

        # 🔹 CSV 파일로 저장
        output_file = os.path.join(output_path, f"{company}_processed.csv")
        merged_data.to_csv(output_file, index=False, encoding='utf-8-sig')

        print(f"✅ {company} 데이터 처리 완료 & 저장됨: {output_file}")
        print(merged_data)
    except Exception as e:
        print(f"❌ {company} 데이터 처리 중 오류 발생: {e}")

print("🎉 모든 회사 데이터 처리 완료!")

# 📌 데이터 경로 설정
data_dir = "C:/Users/smhrd/scm_risk_detector_backend/raison/data"

# 📌 `_processed.csv` 파일만 불러오기
csv_files = [file for file in os.listdir(data_dir) if file.endswith("_processed.csv")]

# 📌 병합할 데이터 리스트
data_frames = []

# 📌 파일 하나씩 불러와서 리스트에 추가
for file in csv_files:
    file_path = os.path.join(data_dir, file)
    df = pd.read_csv(file_path)
    data_frames.append(df)

# 📌 모든 데이터프레임 병합
merged_df = pd.concat(data_frames, ignore_index=True)

# 📌 병합된 데이터 확인
print(merged_df)

# 📌 CSV로 저장 (향후 재사용 가능)
merged_df.to_csv(os.path.join(data_dir, "merged_processed_data.csv"), index=False)
print("✅ 모든 CSV 파일이 병합되었습니다!")
