import os
import pandas as pd
from aggregated_dataset_db import AggregatedDatasetDB

def batch_insert(db, data, symbol, batch_size=1000):
    """
    데이터를 배치로 나누어 데이터베이스에 삽입.
    
    :param db: 데이터베이스 연결 객체
    :param data: 삽입할 데이터 DataFrame
    :param symbol: 심볼 이름
    :param batch_size: 배치 크기
    """
    for start in range(0, len(data), batch_size):
        end = start + batch_size
        batch_data = data.iloc[start:end]
        db.insert(batch_data, symbol)
        print(f"Inserted batch {start} to {end} for symbol {symbol}")

def get_last_date(db, symbol):
    """
    데이터베이스에서 해당 심볼의 가장 최근 날짜를 가져옴.
    
    :param db: 데이터베이스 연결 객체
    :param symbol: 심볼 이름
    :return: 가장 최근 날짜 (datetime 객체) 또는 None
    """
    query = f"SELECT MAX(date) FROM {symbol}"
    result = db.query(query)
    if result and result[0][0]:
        return pd.to_datetime(result[0][0])
    return None

def main(data_path: str, db_config: dict):
    """
    데이터 폴더 내의 모든 Yahoo Finance 데이터를 읽어 데이터베이스에 삽입.
    
    :param data_path: Yahoo Finance 데이터가 저장된 폴더 경로
    :param db_config: 데이터베이스 설정 딕셔너리
    """
    # 데이터베이스 연결 설정
    db = AggregatedDatasetDB(
        db_address=db_config['db_address'],
        user_id=db_config['user_id'],
        pw=db_config['pw'],
        db_name=db_config['db_name'],
        port=int(db_config.get('port', 3307))
    )

    # 데이터 폴더 내의 모든 파일을 읽어 데이터베이스에 삽입
    for country in ['KOR', 'USA']:
        country_path = os.path.join(data_path, country)
        if os.path.isdir(country_path):
            for symbol in os.listdir(country_path):
                symbol_path = os.path.join(country_path, symbol)
                if os.path.isdir(symbol_path):
                    last_date = get_last_date(db, symbol)
                    for file in os.listdir(symbol_path):
                        file_path = os.path.join(symbol_path, file)
                        if file.endswith('.csv'):
                            data = pd.read_csv(file_path).drop_duplicates(keep='last')
                            data['date'] = pd.to_datetime(data['date'])
                            if last_date:
                                data = data[data['date'] > last_date]
                            if not data.empty:
                                try:
                                    batch_insert(db, data, symbol)
                                except Exception as e:
                                    print(f"Error inserting data from {file}: {e}")
    db.close()

if __name__ == "__main__":
    db_config = {
        'db_address': 'project-db-campus.smhrd.com',
        'user_id': 'InvestGenius',
        'pw': '12345',
        'db_name': 'InvestGenius',
        'port': 3307
    }
    data_path = os.path.join(os.path.dirname(__file__), '../../data')
    main(data_path, db_config)
