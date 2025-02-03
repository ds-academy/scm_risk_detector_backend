import os
import json
import pandas as pd
from typing import List

def find_missing_dates(report_path: str, start_date: str, end_date: str) -> List[str]:
    """
    주어진 날짜 범위 내에서 누락된 날짜를 찾아 반환합니다.
    report.json에서 해당 날짜들이 존재하는지 확인합니다.

    :param report_path: 회사의 report.json 파일 경로
    :param start_date: 시작 날짜 (YYYY-MM-DD 형식)
    :param end_date: 종료 날짜(YYYY-MM-DD 형식)
    :return: 누락된 날짜 리스트
    """

    missing_dates = []

    if os.path.exists(report_path):
        with open(report_path, 'r', encoding='utf-8') as f:
            report = json.load(f)

        #     # debug: 보고서에서 pubDate 값을 직접 출력해서 확인해봄
        # print("pubDate in report:")
        # for item in report.values():
        #     if 'pubDate' in item:
        #         print(item['pubDate'])

        for date in pd.date_range(start=start_date, end=end_date):
            date_str = date.strftime('%Y-%m-%d')

            # report 의 각 항목에서 'pubDate' 값을 확인하여 누락된 날짜를 찾음
            found = False
            for item in report.values():
                if 'pubDate' in item and date_str == item['pubDate'][:10]:
                    found = True
                    break
            if not found:
                missing_dates.append(date_str)
    else:
        print(f"File not found at {report_path}. Please check the path.")
    return missing_dates

# 임의로 테스트할 데이터 경로와 날짜 설정
report_path = r'C:\Users\smhrd\scm_risk_detector_backend\data\news\004370\report.json'  # 임의로 경로 설정
start_date = "2025-01-21"
end_date = "2025-01-31"

# 실행 예시
missing_dates = find_missing_dates(report_path, start_date, end_date)
print(missing_dates)