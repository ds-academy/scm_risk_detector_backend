import pymysql
from module.data.database.db_connector_refac import DBConnector
from module.logger import get_logger

logger = get_logger(__name__)


class AlarmingDB(DBConnector):
    """
    사용자에게 알람을 주기 위한 Database 클래스

    """

    def __init__(self, host=None, user=None, password=None, db=None, port=None):
        """
        상위 클래스의 생성자를 호출해 DB 연결 정보를 설정한다.
        """
        super().__init__(host, user, password, db, port)

    def select(self, where: str = None):
        """
        Alarming 테이블에서 데이터를 조회하고 반환한다.

        :param where: 선택적 조건 문자열
        :return: 조회된 데이터(list[dict]) 또는 None
        """
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT * FROM Alarming"
                if where:
                    sql += f" WHERE {where}"
                cursor.execute(sql)
                return cursor.fetchall()
        except pymysql.MySQLError as e:
            logger.error(f"Error executing select: {e}")
            return None

    def insert(self, data: dict):
        """
        Alarming 테이블에 데이터(단일 레코드)를 삽입한다.

        :param data: 삽입할 데이터 딕셔너리 {컬럼명: 값}
        """
        try:
            with self.connection.cursor() as cursor:
                columns = ', '.join(data.keys())
                values = ', '.join(['%s'] * len(data))
                sql = f"INSERT INTO Alarming ({columns}) VALUES ({values})"
                cursor.execute(sql, tuple(data.values()))
            self.connection.commit()
            logger.info("Insert successful.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"Error executing insert: {e}")

    def update(self, data: dict, where: str):
        """
        Alarming 테이블의 특정 조건에 맞는 데이터를 업데이트한다.

        :param data: 업데이트할 데이터 딕셔너리 {컬럼명: 값}
        :param where: 업데이트 조건 문자열
        """
        try:
            set_clause = ', '.join([f"{key}=%s" for key in data.keys()])
            sql = f"UPDATE Alarming SET {set_clause} WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql, tuple(data.values()))
            self.connection.commit()
            logger.info("Update successful.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"Error executing update: {e}")

    def delete(self, where: str):
        """
        Alarming 테이블의 특정 조건에 맞는 데이터를 삭제한다.

        :param where: 삭제 조건 문자열
        """
        try:
            sql = f"DELETE FROM Alarming WHERE {where}"
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
            self.connection.commit()
            logger.info("Delete successful.")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"Error executing delete: {e}")
