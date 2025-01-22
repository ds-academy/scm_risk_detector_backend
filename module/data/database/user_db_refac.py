import pymysql
from module.data.database.db_connector_refac import DBConnector
from module.logger import get_logger

logger = get_logger(__name__)


class UserDB(DBConnector):
    """
    User 테이블에 대한 CRUD 작업을 수행하는 클래스.
    DBConnector를 상속받아 pymysql 연결을 관리한다.
    """

    def __init__(self, host=None, user=None, password=None, db=None, port=None):
        super().__init__(host, user, password, db, port)

    def select(self, where: str = None):
        """
        User 테이블에서 데이터를 조회하고 반환한다.

        :param where: 선택적 조건 문자열
        :return: 조회된 데이터(list[dict]) 또는 None
        """
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM User"
                if where:
                    sql += f" WHERE {where}"
                cursor.execute(sql)
                return cursor.fetchall()
        except pymysql.MySQLError as e:
            logger.error(f"Error executing select: {e}")
            return None

    def insert(self, data: dict):
        """
        User 테이블에 데이터(단일 레코드)를 삽입한다.

        :param data: 삽입할 데이터 딕셔너리 {컬럼명: 값}
        """
        try:
            with self.connection.cursor() as cursor:
                columns = ', '.join(data.keys())
                values = ', '.join(['%s'] * len(data))
                sql = f"INSERT INTO User ({columns}) VALUES ({values})"
                cursor.execute(sql, tuple(data.values()))
            self.connection.commit()
            logger.info("Insert successful.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"Error executing insert: {e}")

    def update(self, data: dict, where: str):
        """
        User 테이블의 특정 조건에 맞는 데이터를 업데이트한다.

        :param data: 업데이트할 데이터 딕셔너리 {컬럼명: 값}
        :param where: 업데이트 조건 문자열
        """
        try:
            set_clause = ', '.join([f"{key}=%s" for key in data.keys()])
            sql = f"UPDATE User SET {set_clause} WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(data.values()))
            self.connection.commit()
            logger.info("Update successful.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"Error executing update: {e}")

    def delete(self, where: str):
        """
        User 테이블의 특정 조건에 맞는 데이터를 삭제한다.

        :param where: 삭제 조건 문자열
        """
        try:
            sql = f"DELETE FROM User WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
            self.connection.commit()
            logger.info("Delete successful.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"Error executing delete: {e}")
