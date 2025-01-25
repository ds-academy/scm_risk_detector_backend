import json
from datetime import datetime


GPT_PROMPT = """
역할: 뉴스 데이터 감성분석 및 키워드 추출 전문가

목표: 
- 제공된 뉴스 데이터에 대한 감성 분석 점수 산출 (0~1 범위)
- 핵심 긍정/부정 키워드 각 5개 추출
- 기업 정보 및 산업군 분류

분석 기준:
1. Sentiment Score (감성 점수)
  - 범위: 0.0 (매우 부정) ~ 1.0 (매우 긍정)
  - 소수점 둘째자리까지 계산

2. 키워드 추출 기준
  - 긍정 키워드: 기업/산업 성장, 실적 개선, 혁신, 투자 등 긍정적 의미를 내포하는 단어
  - 부정 키워드: 위기, 하락, 손실, 리스크 등 부정적 의미를 내포하는 단어
  - 각각 영향력이 큰 상위 5개 단어 선정

출력 형식: JSON
{
   "CompanyName": "기업명",
   "CompanySector": "산업군 분류",
   "Sentiment": "감성 점수 (0.00~1.00)",
   "PositiveKeywords": ["긍정 키워드1", "긍정 키워드2", "긍정 키워드3", "긍정 키워드4", "긍정 키워드5"],
   "NegativeKeywords": ["부정 키워드1", "부정 키워드2", "부정 키워드3", "부정 키워드4", "부정 키워드5"]
}

주의사항:
1. 모든 키워드는 뉴스 본문에 실제 등장한 단어만 선택
2. 산업군은 표준산업분류 기준을 따름
3. 감성 점수는 전체 문맥을 고려하여 산정
4. 특수문자나 줄바꿈 없이 JSON 형식으로 반환
"""


def format_recent_chat_history(chat_history, n=5):
    # 시간순으로 정렬 (타임스탬프가 문자열인 경우를 대비해 datetime 객체로 변환)
    sorted_history = sorted(
        chat_history,
        key=lambda x: (
            datetime.fromisoformat(x["timestamp"])
            if isinstance(x["timestamp"], str)
            else x["timestamp"]
        ),
    )

    # 최근 N개 선택
    recent_history = sorted_history[-n:]

    # ChatGPT API 형식으로 변환
    formatted_messages = []
    for entry in recent_history:
        if entry["speaker"] == "user":
            role = "user"
        elif entry["speaker"] in ["llama", "gpt"]:
            role = "assistant"
        else:
            print(f"Warning: Unknown speaker type: {entry['speaker']}")
            continue  # 알 수 없는 speaker는 무시

        formatted_messages.append({"role": role, "content": entry["message"]})

    return formatted_messages
