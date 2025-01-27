import pymysql
from module.data.database.db_connector import DBConnector
from module.logger import get_logger

logger = get_logger(__name__)


class NewsDataInserter(DBConnector):
    """
    News Data Inserter 담당 클래스.
    - 기본 insert_* 메서드는 NEWS_MAIN, NEWS_COMPANY, NEWS_SENTIMENT에 특화
    - select / update / delete는 NEWS_MAIN 테이블 기준 예시
    """

    def select(self, where: str = None):
        """
        NEWS_MAIN 테이블에서 데이터를 조회하여 반환.
        :param where: 선택적 조건 문자열 (예: "NEWS_ID=10")
        :return: 조회된 데이터(list[dict]) or None
        """
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM NEWS_MAIN"
                if where:
                    sql += f" WHERE {where}"
                cursor.execute(sql)
                return cursor.fetchall()
        except pymysql.MySQLError as e:
            logger.error(f"[NewsDataInserter] Error executing select: {e}")
            return None

    def update(self, data: dict, where: str):
        """
        NEWS_MAIN 테이블에서 특정 조건에 맞는 컬럼들을 업데이트.
        :param data: {컬럼명: 값}
        :param where: 업데이트 조건 문자열 (예: "NEWS_ID=10")
        """
        try:
            set_clause = ', '.join([f"{col}=%s" for col in data.keys()])
            sql = f"UPDATE NEWS_MAIN SET {set_clause} WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(data.values()))
            self.connection.commit()
            logger.info("[NewsDataInserter] Update successful.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"[NewsDataInserter] Error executing update: {e}")

    def delete(self, where: str):
        """
        NEWS_MAIN 테이블에서 특정 조건에 맞는 행을 삭제.
        :param where: 삭제 조건 문자열 (예: "NEWS_ID=10")
        """
        try:
            sql = f"DELETE FROM NEWS_MAIN WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
            self.connection.commit()
            logger.info("[NewsDataInserter] Delete successful.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"[NewsDataInserter] Error executing delete: {e}")

    # --------------------- NEWS_MAIN (INSERT) ---------------------
    def insert_news_main(self,
                         title: str,
                         content: str,
                         pub_date,        # Python datetime or string
                         source: str,
                         news_api: str) -> int:
        """
        INSERT INTO NEWS_MAIN (TITLE, CONTENT, PUB_DATE, SOURCE, NEWS_API)
        => AUTO_INCREMENT => return new NEWS_ID
        """
        query = """
        INSERT INTO NEWS_MAIN
          (TITLE, CONTENT, PUB_DATE, SOURCE, NEWS_API)
        VALUES
          (%s, %s, %s, %s, %s)
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (title, content, pub_date, source, news_api))
        self.connection.commit()
        new_id = self.get_last_insert_id()
        logger.info(f"[NEWS_MAIN] Inserted => NEWS_ID={new_id}")
        return new_id

    # --------------------- NEWS_COMPANY (INSERT) ---------------------
    def insert_news_company(self, news_id: int, company_code: str):
        """
        INSERT INTO NEWS_COMPANY(NEWS_ID, COMPANY_CODE)
        """
        query = """
        INSERT INTO NEWS_COMPANY (NEWS_ID, COMPANY_CODE)
        VALUES (%s, %s)
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (news_id, company_code))
        self.connection.commit()
        logger.info(f"[NEWS_COMPANY] Inserted (NEWS_ID={news_id}, COMPANY_CODE={company_code})")

    # --------------------- NEWS_SENTIMENT (INSERT) ---------------------
    def insert_news_sentiment(self,
                              news_id: int,
                              sentiment_val: float,
                              pos_str: str,
                              neg_str: str,
                              pub_date):
        """
        NEWS_SENTIMENT(NEWS_ID PK, SENTIMENT(DECIMAL(7,4)), POSITIVE_KEYWORDS, NEGATIVE_KEYWORDS, PUB_DATE)
        """
        query = """
        INSERT INTO NEWS_SENTIMENT
         (NEWS_ID, SENTIMENT, POSITIVE_KEYWORDS, NEGATIVE_KEYWORDS, PUB_DATE)
        VALUES
         (%s, %s, %s, %s, %s)
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, (news_id, sentiment_val, pos_str, neg_str, pub_date))
        self.connection.commit()
        logger.info(f"[NEWS_SENTIMENT] Inserted NEWS_ID={news_id}, sentiment={sentiment_val}")
