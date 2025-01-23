import os
import sys
import urllib.request
import urllib.parse
import json

client_id = "oJ9H3ww68UJPMlQErEb_"
client_secret = "gk7pKWpwA5"

encText = urllib.parse.quote("삼성전자")
url = "https://openapi.naver.com/v1/search/news?query=" + encText # JSON 결과
print(url)
# API 요청 객체 생성
request = urllib.request.Request(url)
# 요청 헤더에 클라이언트 ID와 시크릿 추가
request.add_header("X-Naver-Client-Id", client_id)
request.add_header("X-Naver-Client-Secret", client_secret)
# API 요청 보내기
response = urllib.request.urlopen(request)
# 응답 코드 확인
rescode = response.getcode()

# 404(페이지 없음)이면 에러 출력
# 500(서버 에러)이면 에러 출력

# 응답 코드가 200(성공)인 경우
if (rescode == 200):
    response_body = response.read()
    response_body = json.loads(response_body.decode('utf-8'))

    print(response_body)  # 응답 본문 출력

    for item in response_body['items']:
        title = item['title']
        description = item['description']

        print(title)
        print(description)

else:
    print("Error Code" + rescode)