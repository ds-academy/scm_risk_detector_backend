# SCM (Supply Chain Management) Risk Detector 
- 개별 기업의 SCM Risk를 실시간으로 탐지하여 투자자에게 전달하는 웹 서비스의 백엔드 기능
- 개별 기업의 SCM 정보, 주가, 원자재, 뉴스 데이터들을 종합하여 분석 및 예측 
- 이를 기반으로 SCM Risk를 탐지하고, 투자자에게 실시간 위험 수준을 제공하는 서비스 
- 본 프로젝트는 '스마트인재개발원'에서 진행하는 'KDT - 빅데이터 분석서비스 개발자 과정'의 일환으로 진행되었습니다. 

# Core 기능 소개 

## 1. Data Pipelines
- Data Pipeline은 데이터의 수집과 처리를 담당합니다
- 데이터의 호출과 저장을 담당합니다.

### 1.1. News Data 
- (1) `Naver News API`를 이용해 뉴스 데이터를 수집합니다.
- (2) 수집된 News 데이터에서 originallink를 추출하고 이를 python `newspaper3` library 를 이용해 본문을 crawling 합니다.
- (3) 수집된 본문 데이터를 `Open AI GPT-4o-mini` 모델을 이용해 정보를 추출합니다.
- (4) 추출된 정보를 `MSSQL Server`에 저장합니다.

### 1.2 Stock Data
- (1) 미국 시장의 경우 `Yahoo Finance API` 한국 시장의 경우 `FinanceDataReader`를 이용해 주가 데이터를 수집합니다.
- (2) 수집된 미국, 한국 시장 데이터를 `MSSQL Server`에 저장합니다.

### 1.3 SCM Data
- (1) SCM 데이터의 경우 전자공시시스템에서 수집합니다.
- (2) 현재 본 프로젝트에서는 전자공시시스템에서 자동으로 수집되는 기능은 구현되어 있지 않습니다. 


## 2. Data Analysis
- Data Analysis는 수집된 데이터를 분석하고, 모델링을 담당합니다.


    