import os
import pandas as pd
import pymysql
from db_connector import DBConnector

class AggregatedDatasetDB(DBConnector):
    def __init__(self, db_address, user_id, pw, db_name, port):
        self.connection = pymysql.connect(
            host=db_address,
            user=user_id,
            password=pw,
            db=db_name,
            port=port,
            cursorclass=pymysql.cursors.DictCursor,
            client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS
        )

    def select(self, where: str = None):
        """
        AggregatedDataset 테이블에서 모든 데이터를 조회하고 반환.
        
        :param where: 선택적 조건 문자열
        :return: 조회된 데이터의 리스트
        """
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM AggregatedDataset"
                if where:
                    sql += f" WHERE {where}"
                cursor.execute(sql)
                result = cursor.fetchall()
                return result
        except pymysql.MySQLError as e:
            print(f"Error executing select: {e}")
            return None

    def insert(self, data, symbol):
        """
        Yahoo Finance 데이터를 AggregatedDataset 테이블에 삽입.
        
        :param data: Yahoo Finance 데이터 DataFrame
        :param symbol: 심볼 이름
        """
        if data.empty:
            print(f"No data found for {symbol}")
            return

        data = data.copy()  # 경고를 피하기 위해 데이터의 복사본을 생성
        data.loc[:, 'date'] = pd.to_datetime(data['date'])  # date 컬럼을 datetime 타입으로 변환

        for index, row in data.iterrows():
            record = {
                'asset_id': index + 1,  # 자동 증가 값 대신 인덱스 + 1 사용
                'trade_date': row['date'].date(),  # .date()를 사용하여 시간 부분 제거
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume'],
                'exchange': 'NASDAQ',  # 고정값으로 설정
                'asset_type': 'EQUITY',  # 고정값으로 설정
                'national': 'US',  # 고정값으로 설정
                'asset_name': symbol  # 심볼명 설정
            }
            try:
                with self.connection.cursor() as cursor:
                    columns = ', '.join(record.keys())
                    values = ', '.join(['%s'] * len(record))
                    sql = f"INSERT INTO AggregatedDataset ({columns}) VALUES ({values});"
                    cursor.execute(sql, tuple(record.values()))
                    self.connection.commit()
            except pymysql.MySQLError as e:
                print(f"Error executing insert: {e}")

    def update(self, data: dict, where: str):
        """
        AggregatedDataset 테이블의 특정 조건에 맞는 데이터를 업데이트.
        
        :param data: 업데이트할 데이터 딕셔너리
        :param where: 업데이트 조건 문자열
        """
        try:
            with self.connection.cursor() as cursor:
                set_clause = ', '.join([f"{key}=%s" for key in data.keys()])
                sql = f"UPDATE AggregatedDataset SET {set_clause} WHERE {where};"
                cursor.execute(sql, tuple(data.values()))
                self.connection.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing update: {e}")

    def delete(self, where: str):
        """
        AggregatedDataset 테이블의 특정 조건에 맞는 데이터를 삭제.
        
        :param where: 삭제 조건 문자열
        """
        try:
            with self.connection.cursor() as cursor:
                sql = f"DELETE FROM AggregatedDataset WHERE {where};"
                cursor.execute(sql)
                self.connection.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing delete: {e}")

    def close(self):
        if self.connection:
            self.connection.close()

def get_last_date(db, symbol):
    """
    데이터베이스에서 해당 심볼의 가장 최근 날짜를 가져옴.
    
    :param db: 데이터베이스 연결 객체
    :param symbol: 심볼 이름
    :return: 가장 최근 날짜 (datetime 객체) 또는 None
    """
    query = f"SELECT MAX(trade_date) as last_date FROM AggregatedDataset WHERE asset_name = %s"
    result = db.execute_query(query, (symbol,))
    if result and result[0]['last_date']:
        return pd.to_datetime(result[0]['last_date'], utc=True)  # UTC 시간대로 변환
    return None

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
                            data['date'] = pd.to_datetime(data['date'], utc=True)  # UTC 시간대로 변환
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
