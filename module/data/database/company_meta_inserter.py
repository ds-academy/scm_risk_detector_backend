import pymysql
from module.data.database.db_connector import DBConnector
from module.logger import get_logger

logger = get_logger(__name__)


class CompanyMetaInserter(DBConnector):
    """
    COMPANY_META 테이블에 대한 CRUD 담당.
    """

    def select(self, where: str = None):
        """
        COMPANY_META에서 조회.
        :param where: "COMPANY_CODE='AAPL'"
        :return: list[dict] or None
        """
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM COMPANY_META"
                if where:
                    sql += f" WHERE {where}"
                cursor.execute(sql)
                return cursor.fetchall()
        except pymysql.MySQLError as e:
            logger.error(f"[CompanyMetaInserter] select error: {e}")
            return None

    def insert(self, data: dict):
        """
        data = {
          "COMPANY_CODE": ...,
          "COMPANY_NAME": ...,
          "COUNTRY": ...,
          "SECTOR": ...
        }
        """
        try:
            cols = ", ".join(data.keys())
            vals = ", ".join(["%s"] * len(data))
            sql = f"INSERT INTO COMPANY_META ({cols}) VALUES ({vals})"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(data.values()))
            self.connection.commit()
            logger.info(f"[CompanyMetaInserter] Insert success: {data['COMPANY_CODE']}")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"[CompanyMetaInserter] Insert error: {e}")

    def update(self, data: dict, where: str):
        """
        :param data: { "COMPANY_NAME":..., "SECTOR":..., ... }
        :param where: "COMPANY_CODE='AAPL'"
        """
        try:
            set_clause = ", ".join([f"{k}=%s" for k in data.keys()])
            sql = f"UPDATE COMPANY_META SET {set_clause} WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(data.values()))
            self.connection.commit()
            logger.info("[CompanyMetaInserter] Update success.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"[CompanyMetaInserter] Update error: {e}")

    def delete(self, where: str):
        """
        :param where: "COMPANY_CODE='AAPL'"
        """
        try:
            sql = f"DELETE FROM COMPANY_META WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
            self.connection.commit()
            logger.info("[CompanyMetaInserter] Delete success.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"[CompanyMetaInserter] Delete error: {e}")
