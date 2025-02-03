import os
import time
import pandas as pd
from collections import Counter

import yaml
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from module.logger import get_logger

logger = get_logger(__name__)

def fetch_data_via_selenium(missing_dates:list, query:str) -> pd.DataFrame:
    """
    selenium을 이용하여 누락된 날짜의 뉴스 데이터를 크롤링하는 메서드
    :param missing_dates:
    :param query:
    :return:
    """
    # Selenium 설정
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # 헤드리스 모드로 실행 (UI없이 실행)
    service = Service("C:/Users/smhrd/Downloads/chromedriver-win64/chromedriver-win64/chromedriver.exe")  # chromedriver 경로 설정
    driver = webdriver.Chrome(service=service, options=chrome_options)

    data = []

    for date in missing_dates:
        # 날짜에 해당하는 네이버 뉴스 url 생성
        url = f"https://search.naver.com/search.naver?query={query}+{date}&where=news"
        driver.get(url)

        time.sleep(1.7)

        # 날짜별 뉴스 링크 크롤링
        articles = driver.find_elements(By.CSS_SELECTOR, ".news_tit")

        # WebElement 리스트에서 각 항목을 하나씩 가져와 처리
        count = 0
        for article in articles:
            if count >= 2:  # 하루기사 최대 2개까지만 크롤링 ( 하지만 7개가 크롤링되는중 원인파악 곤란... 해결방안 제시해주시면 감사)
                break
            link = article.get_attribute("href")
            title = article.text

            # 크롤링한 데이터 추가
            data.append({
                "date": date,
                "title": title,
                "originallink": link,
                "link": link
            })
            count += 1

    driver.quit()

    # 크롤링한 데이터를 DF으로 변환
    if not data:
        logger.warning("No articles found for date {date}")
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # pubDate를 datetime 변환
    df['pubDate'] = pd.to_datetime(df['date'], errors='coerce')

    # 중복된 데이터 제거
    df.drop_duplicates(subset=['originallink'], keep="last", inplace=True)

    # pubDate 기준으로 정렬
    df.sort_values("pubDate", inplace=True)

    logger.info(f"Crawled {len(df)} news articles via selenium for {query}")

    # # 한 번이라도 크롤링 한 종목은 기록후 영구 크롤링 대상 제외
    # with open('configs/datasources/naver_news.yaml', 'r+', encoding='utf-8') as file:
    #     try:
    #         data = yaml.load(file, Loader=yaml.FullLoader)
    #     except yaml.YAMLError as e:
    #         data = {}  # 예외가 발생하면 빈 딕셔너리로 초기화
    #     # completed_crawling에 query 추가
    #     if 'completed_crawling' not in data:
    #         data['completed_crawling'] = []
    #     if query not in data['completed_crawling']:
    #         data['completed_crawling'].append(query)
    #         file.seek(0)  # 파일 포인터를 처음으로 되돌림
    #         yaml.dump(data, file, default_flow_style=False, allow_unicode=True)
    #     logger.info(f"Successfully added {query} to completed_crawling.")

    return df








