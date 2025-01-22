from module.data.database.db_connector_refac import DBConnector
from module.logger import get_logger

logger = get_logger(__name__)


class MemberDBConnector(DBConnector):
    """
    Member 관련 DB 연산을 담당하는 클래스.
    DBConnector 상속으로 pymysql 연결 및 execute_query 기능을 제공받는다.
    """

    def __init__(self, host=None, user=None, password=None, db=None, port=None):
        super().__init__(host, user, password, db, port)

    def insert(self, query, params=None):
        """
        데이터 삽입 쿼리를 실행한다.

        :param query: INSERT SQL문
        :param params: 파라미터(옵셔널)
        :return: 삽입된 레코드 ID 또는 None
        """
        try:
            self.execute_query(query, params)
            last_id = self.get_last_insert_id()
            logger.info(f"Insert successful. ID={last_id}")
            return last_id
        except Exception as e:
            logger.error(f"Error in insert operation: {e}")
            return None

    def select(self, query, params=None):
        """
        데이터 조회 쿼리를 실행한다.

        :param query: SELECT SQL문
        :param params: 파라미터(옵셔널)
        :return: 조회 결과(list of dict)
        """
        try:
            return self.execute_query(query, params)
        except Exception as e:
            logger.error(f"Error in select operation: {e}")
            return []

    def update(self, query, params=None):
        """
        데이터 업데이트 쿼리를 실행한다.

        :param query: UPDATE SQL문
        :param params: 파라미터(옵셔널)
        """
        try:
            self.execute_query(query, params)
            logger.info("Update successful.")
        except Exception as e:
            logger.error(f"Error in update operation: {e}")

    def delete(self, query, params=None):
        """
        데이터 삭제 쿼리를 실행한다.

        :param query: DELETE SQL문
        :param params: 파라미터(옵셔널)
        """
        try:
            self.execute_query(query, params)
            logger.info("Delete successful.")
        except Exception as e:
            logger.error(f"Error in delete operation: {e}")

    def insert_user(self, user_data):
        """
        USERS_TB 테이블에 유저 정보를 삽입한다.

        :param user_data: {컬럼명: 값} 형태의 딕셔너리
        """
        query = """
            INSERT INTO USERS_TB (
                user_id, user_pw, user_nickname, user_name, 
                user_phone, user_email, join_date
            )
            VALUES (
                %(user_id)s, %(user_pw)s, %(user_nickname)s, %(user_name)s, 
                %(user_phone)s, %(user_email)s, %(join_date)s
            )
        """
        return self.insert(query, user_data)

    def get_user_by_id(self, user_id):
        """
        특정 user_id로 USERS_TB에서 사용자 정보를 조회한다.

        :param user_id: 조회할 user_id
        :return: 사용자 정보(dict) 또는 None
        """
        query = """
            SELECT user_id, user_pw 
            FROM USERS_TB 
            WHERE user_id = %(user_id)s
        """
        result = self.select(query, {"user_id": user_id})
        return result[0] if result else None
