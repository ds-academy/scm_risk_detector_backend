import pymysql
import pandas as pd
from module.data.database.db_connector import DBConnector
from module.logger import get_logger

logger = get_logger(__name__)


class StockDataInserter(DBConnector):
    """
    Stock Data (USA, KOR) Inserter + CRUD for STOCK_PRICE.
    """

    def select(self, where: str = None):
        """
        STOCK_PRICE 테이블에서 데이터를 조회하여 반환.
        :param where: 선택적 조건 문자열 (예: "COMPANY_CODE='005930'")
        :return: 조회된 데이터(list[dict]) or None
        """
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM STOCK_PRICE"
                if where:
                    sql += f" WHERE {where}"
                cursor.execute(sql)
                return cursor.fetchall()
        except pymysql.MySQLError as e:
            logger.error(f"[StockDataInserter] Error executing select: {e}")
            return None

    def update(self, data: dict, where: str):
        """
        STOCK_PRICE 테이블에서 특정 조건에 맞는 컬럼들을 업데이트.
        :param data: {컬럼명: 값}
        :param where: 업데이트 조건 (예: "COMPANY_CODE='005930' AND DATE='2025-01-25'")
        """
        try:
            set_clause = ', '.join([f"{col}=%s" for col in data.keys()])
            sql = f"UPDATE STOCK_PRICE SET {set_clause} WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(data.values()))
            self.connection.commit()
            logger.info("[StockDataInserter] Update successful.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"[StockDataInserter] Error executing update: {e}")

    def delete(self, where: str):
        """
        STOCK_PRICE 테이블에서 특정 조건에 맞는 행을 삭제.
        :param where: 예: "COMPANY_CODE='005930' AND DATE='2025-01-25'"
        """
        try:
            sql = f"DELETE FROM STOCK_PRICE WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
            self.connection.commit()
            logger.info("[StockDataInserter] Delete successful.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"[StockDataInserter] Error executing delete: {e}")

    # -------------------------- 주가 데이터(STOCK_PRICE) --------------------------
    def insert_stock_price(self, company_code: str, df: pd.DataFrame):
        """
        STOCK_PRICE(회사코드, date, open, high, low, close) 삽입
        df: columns=[date, open, high, low, close]
        """
        query = """
        INSERT INTO STOCK_PRICE (COMPANY_CODE, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        rows = []

        df["volume"] = df["volume"].fillna(0)
        df["volume"] = df["volume"].clip(lower=0)
        df["volume"] = df["volume"].astype(int)

        for _, row in df.iterrows():
            dt_val = row["date"]
            if hasattr(dt_val, "to_pydatetime"):
                dt_val = dt_val.to_pydatetime()
            rows.append((
                company_code,
                dt_val,
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"])
            ))

        if not rows:
            logger.info(f"[StockDataInserter] No rows to insert for {company_code}.")
            return
        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(query, rows)
            self.connection.commit()
            logger.info(f"[{company_code}] Inserted {len(rows)} rows into STOCK_PRICE.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"[StockDataInserter] Error inserting stock price: {e}")
