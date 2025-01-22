import os
import pandas as pd
import pymysql
from pymysql.constants import CLIENT
from module.data.database.db_connector_refac import DBConnector
from module.logger import get_logger

logger = get_logger(__name__)


class ChartDB(DBConnector):
    """

    ChartDB
    SQL Database에 저장하기 위한 CRUD 기능 구현
    주로 기업의 주식가격, 지수, 원자재가격(상품가격, SCM과 관련된) 데이터를 입력하는 용도임
    테이블 스키마, 테이블 구조에 따라 아래 코드 수정이 필요

    - Store 'Chart' Data in DB
    - 'Chart' Data : Stock/Index/Commodity Price Data
    """

    def __init__(self, db_address, user_id, pw, db_name, port):
        super().__init__(
            host=db_address,
            user=user_id,
            password=pw,
            db=db_name,
            port=port
        )

    def select(self, where: str = None):
        """
        ChartDB 테이블에서 데이터를 조회하고 반환한다.

        :param where
        :return: list
        """
        try:
            with self.connection.cursor() as cursor:
                query = "SELECT * FROM [The Name of Chart DB Table]"
                if where:
                    query += f" WHERE {where}"
                cursor.execute(query)
                return cursor.fetchall()
        except pymysql.MySQLError as e:
            logger.error(f"Error executing select: {e}")
            return None

    def insert(self, data: pd.DataFrame, symbol: str):
        """
        Insert 'data' to DB

        :param data: pandas DataFrame
        :param symbol: 심볼 이름
        """
        if data.empty:
            logger.info(f"No data found for symbol: {symbol}")
            return

        # 데이터 전처리
        data = data.copy()
        data['date'] = pd.to_datetime(data['date']).dt.date  # date 컬럼을 date 타입으로 변환

        # 삽입할 레코드 목록 변환
        records = []
        for index, row in data.iterrows():
            records.append((
                index + 1,  # 예시로 인덱스 + 1을 ID 대용으로 사용
                row['date'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume'],
                'NASDAQ',  # 고정값
                'EQUITY',  # 고정값
                'US',  # 고정값
                symbol
            ))

        # TODO : Fix real chart db columns and placeholders
        columns = (
            "asset_id, trade_date, open, high, low, close, "
            "volume, exchange, asset_type, national, asset_name"
        )
        placeholders = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
        insert_query = f"""
            INSERT INTO AggregatedDataset ({columns})
            VALUES ({placeholders})
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(insert_query, records)
            self.connection.commit()
            logger.info(f"{len(records)} rows inserted for symbol: {symbol}")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"Error executing insert for symbol {symbol}: {e}")

    def update(self, data: dict, where: str):
        """
        :param data: 업데이트할 데이터 딕셔너리 {컬럼: 값}
        :param where: 업데이트 조건 문자열 (예: "asset_id=10")
        """
        if not data:
            logger.warning("No data provided for update.")
            return

        try:
            set_clause = ', '.join([f"{key}=%s" for key in data.keys()])
            query = f"UPDATE AggregatedDataset SET {set_clause} WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(query, tuple(data.values()))
            self.connection.commit()
            logger.info(f"Update success where {where}")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"Error executing update: {e}")

    def delete(self, where: str):
        """
        :param where: 삭제 조건 문자열
        """
        try:
            query = f"DELETE FROM AggregatedDataset WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(query)
            self.connection.commit()
            logger.info(f"Delete success where {where}")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"Error executing delete: {e}")

    def close(self):
        """DB 연결을 닫는다."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed.")


def get_last_date(db: DBConnector, symbol: str):
    """
    데이터베이스에서 해당 심볼의 가장 최근 날짜를 가져온다.

    :param db: 데이터베이스 연결 객체 (DBConnector 상속 클래스)
    :param symbol: 심볼 이름
    :return: 가장 최근 날짜 (datetime 객체) 또는 None
    """
    query = "SELECT MAX(trade_date) AS last_date FROM AggregatedDataset WHERE asset_name = %s"
    result = db.execute_query(query, (symbol,))
    if result and result[0]['last_date']:
        return pd.to_datetime(result[0]['last_date'], utc=True)
    return None


def batch_insert(db: ChartDB, data: pd.DataFrame, symbol: str, batch_size: int = 1000):
    """
    데이터를 배치로 나누어 데이터베이스에 삽입한다.

    :param db: 데이터베이스 연결 객체 (AggregatedDatasetDB)
    :param data: 삽입할 데이터 (pandas DataFrame)
    :param symbol: 심볼 이름
    :param batch_size: 배치 크기(기본값 1000)
    """
    total_len = len(data)
    for start in range(0, total_len, batch_size):
        end = start + batch_size
        batch_data = data.iloc[start:end]
        db.insert(batch_data, symbol)
        logger.info(f"Inserted batch {start} to {end} (total {total_len}) for symbol {symbol}")


def main(data_path: str, db_config: dict):
    """
    데이터 폴더 내의 모든 Yahoo Finance 데이터를 읽어 데이터베이스에 삽입한다.

    :param data_path: Yahoo Finance 데이터가 저장된 폴더 경로
    :param db_config: 데이터베이스 설정 딕셔너리
    """
    with ChartDB(
            db_address=db_config['db_address'],
            user_id=db_config['user_id'],
            pw=db_config['pw'],
            db_name=db_config['db_name'],
            port=int(db_config.get('port', 3307))
    ) as db:
        # KOR, USA 폴더 순회
        for country in ['KOR', 'USA']:
            country_path = os.path.join(data_path, country)
            if not os.path.isdir(country_path):
                continue

            # 국가 폴더 안의 모든 심볼 폴더 처리
            for symbol in os.listdir(country_path):
                symbol_path = os.path.join(country_path, symbol)
                if not os.path.isdir(symbol_path):
                    continue

                # 해당 심볼의 가장 최근 날짜 조회
                last_date = get_last_date(db, symbol)

                # 심볼 폴더 내 모든 csv 파일을 읽어들여 DB 삽입
                for file in os.listdir(symbol_path):
                    file_path = os.path.join(symbol_path, file)
                    if not file.endswith('.csv'):
                        continue

                    try:
                        data = pd.read_csv(file_path).drop_duplicates(keep='last')
                        # UTC 기준으로 변환
                        data['date'] = pd.to_datetime(data['date'], utc=True)

                        # 이미 입력된 날짜 이후의 데이터만 필터링
                        if last_date:
                            data = data[data['date'] > last_date]

                        if data.empty:
                            logger.info(f"No new data in file {file} for symbol {symbol}")
                            continue

                        batch_insert(db, data, symbol)
                    except Exception as e:
                        logger.error(f"Error inserting data from {file_path}: {e}")


if __name__ == "__main__":
    config = {
        'db_address': 'project-db-campus.smhrd.com',
        'user_id': 'InvestGenius',
        'pw': '12345',
        'db_name': 'InvestGenius',
        'port': 3307
    }
    # 실행 파일 기준 상대 경로
    data_path = os.path.join(os.path.dirname(__file__), '../../data')

    main(data_path, config)
