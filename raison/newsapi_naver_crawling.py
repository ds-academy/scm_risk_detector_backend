import os
import sys
import urllib.request
import urllib.parse
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from newspaper import Article

# 네이버 API 클라이언트 ID와 시크릿
client_id = "oJ9H3ww68UJPMlQErEb_"
client_secret = "gk7pKWpwA5"
encText = urllib.parse.quote(input())

# 네이버뉴스 검색 API URL(JSON 결과)
# query = "파이썬"
# encText = urllib.parse.quote(query)
url = "https://openapi.naver.com/v1/search/news?query=" + encText 
# API 요청 객체 생성
request = urllib.request.Request(url)
# 요청 헤더에 클라이언트 ID와 시크릿 추가
request.add_header("X-Naver-Client-Id", client_id)
request.add_header("X-Naver-Client-Secret", client_secret)
# API 요청 보내기
response = urllib.request.urlopen(request)
# 응답 코드 확인
rescode = response.getcode()

# 응답 코드가 200(성공)인 경우
if(rescode == 200):
    response_body = response.read()
    result = json.loads(response_body.decode('utf-8'))
    urls = [item['originallink'] for item in result['items']]

else:
    print("Error Code" + rescode)  

# 뉴스 제목과 내용 추출 함수
def fetch_content(url):
    try:
        article = Article(url, language="ko")
        article.download()
        article.parse()
        return {"url":url, "title":article.title, "content":article.text}
    except Exception as e :
        return {"url":url, "error": str(e)}

# 모든 링크 크롤링
results = [fetch_content(url) for url in urls]