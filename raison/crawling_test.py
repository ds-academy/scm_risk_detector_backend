import pandas as pd

from data.news.fetch_data_via_selenium import fetch_data_via_selenium

# 예시 데이터 (테스트용 날짜 및 키워드)
missing_dates = ["2025-01-21", "2025-01-25", "2025-01-26", "2025-01-27", "2025-01-28"]
query = "농심"  # 테스트할 키워드

# 크롤링 함수 호출
df = fetch_data_via_selenium(missing_dates, query)

# 결과 출력
print(df.values)

