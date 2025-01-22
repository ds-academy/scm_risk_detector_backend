# The-First-Step
LLM 기반의 투자유형 추천 시스템

## 1. Data Pipelines

### twelveData
- Not Implemented yet.  
  - TBD 

### Yahoo Finance
- pip install yfinance
-  사용법
    - _configs/yahoo_config.ini_ 에 다운로드 받고자 하는 주식 symbol(ticker) 및 거래소 작성
    - _python run_yahoo.py_  를 통해 원하는 경로에 파일 다운로드 가능 
    - _python load_yahoo.py_ 를 수행하여 다운로드 된 일일 가격 데이터를 메모리애 로드 가능


### FinanceDataReader 
- pip install finance-datareader
- pip install plotly 
- 사용법
  - define _configs/fdr_config.ini_
  - _python run_fdr_korea.py 
  - _python load_fdr_korea.py 


### insert_yahoo_data_to_db.py
- 사용법
  - pip install pymysql 먼저하기
  - python insert_yahoo_data_to_db.py 를 통해 DB로 저장



## 2. LLM 
- TBD



## 3. Trading Strategy
- Key concepts
  - algo : fit (train model), predict
  - strategy : input DataPipeline, Algo  -> execute 
  - stretegypool : backtest list of strategies class and output best set of strategies at time T 


    