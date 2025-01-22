import requests
from bs4 import BeautifulSoup
import time

# 헤드라인을 가져올 함수
def get_headlines():
    url = "https://finance.naver.com/news/news_list.naver?mode=RANK"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    headlines = [item.get_text().strip() for item in soup.select("div.hotNewsList ul li a")] 

    return headlines[:10] # 가장많이 본 뉴스 10개 헤드라인만 추출

# 메인 함수
def main() :
    while True : # 무한 루프 (10개 헤드라인 하니씩 반환후 다시 10개 추출하러~)
        headlines = get_headlines()
        for headline in headlines :
            print(headline)
            time.sleep(10)  # 10초단위로 헤드라인 하나씩 반환
        print("Feeching new headlines...\n") 

if __name__ == "__main__" :
    main() 