import logging
import pymysql
from abc import ABC, abstractmethod
from module.data.database.db_config import db_config
from module.logger import get_logger

logger = get_logger(__name__)

class DBConnector(ABC):
    """
    pymysql을 이용한 DB 접속 및 기본적인 쿼리 실행을 담당하는 추상 클래스.
    구체적인 select, update, delete 로직은 하위 클래스에서 구현해야 한다.
    """

    def __init__(self, host=None, user=None, password=None, db=None, port=None):
        """
        DBConnector 생성자.

        :param host: DB 서버 주소(기본값: db_config['host'])
        :param user: DB 접속 유저명(기본값: db_config['user'])
        :param password: DB 접속 패스워드(기본값: db_config['password'])
        :param db: 접속할 DB 이름(기본값: db_config['database'])
        :param port: DB 포트(기본값: db_config['port'])
        """
        # 인자가 None이면 db_config에서 기본값을 가져온다.
        if host is None:
            host = db_config['host']
        if user is None:
            user = db_config['user']
        if password is None:
            password = db_config['password']
        if db is None:
            db = db_config['database']
        if port is None:
            port = db_config['port']

        self.connection = None
        self._connect(host, user, password, db, port)

    def _connect(self, host, user, password, db, port):
        """
        실제 DB 연결을 수행하는 내부 메서드.
        예외 처리 및 로그를 통해 연결 상태를 확인한다.
        """
        try:
            self.connection = pymysql.connect(
                host=host,
                user=user,
                password=password,
                db=db,
                port=port,
                cursorclass=pymysql.cursors.DictCursor,
                client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS
            )
            logger.info("Database connection established.")
        except pymysql.MySQLError as e:
            logger.error(f"Error connecting to database: {e}")
            self.connection = None

    def __enter__(self):
        """
        with문 진입 시 호출되는 매직 메서드.
        예:
            with DBConnector(...) as db:
                db.execute_query(...)
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        with문 종료 시 자동으로 호출되어 DB 연결을 닫는다.
        예외 발생 여부(exc_type, exc_val, exc_tb)와 관계없이 항상 close() 실행.
        """
        self.close()

    def execute_query(self, query, params=None):
        """
        쿼리를 실행하고, 결과를 fetch하여 반환한다.
        커밋(commit)/롤백(rollback)을 통해 트랜잭션을 처리한다.

        :param query: 실행할 SQL 쿼리문
        :param params: 쿼리에 바인딩할 파라미터(tuple or dict 등)
        :return: 조회된 데이터(list of dict). 조회 결과가 없거나 오류 발생 시 빈 리스트 반환.
        """
        if not self.connection:
            logger.error("No database connection.")
            return []

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()
                return cursor.fetchall()
        except pymysql.MySQLError as e:
            logger.error(f"Error executing query: {e}")
            self.connection.rollback()
            return []

    def get_last_insert_id(self):
        """
        최근에 AUTO_INCREMENT가 적용된 레코드의 ID를 가져온다.
        (테이블에 AUTO_INCREMENT 칼럼이 있어야 함)

        :return: 마지막으로 입력된 AUTO_INCREMENT 값(int) 또는 None
        """
        if not self.connection:
            logger.error("No database connection.")
            return None

        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT LAST_INSERT_ID() AS last_id")
                result = cursor.fetchone()
                return result['last_id'] if result else None
        except pymysql.MySQLError as e:
            logger.error(f"Error fetching last insert id: {e}")
            return None

    @abstractmethod
    def select(self, where: str = None):
        """
        하위 클래스에서 구현
        """
        pass

    @abstractmethod
    def update(self, data: dict, where: str):
        """
        하위 클래스에서 구현
        """
        pass

    @abstractmethod
    def delete(self, where: str):
        """
        하위 클래스에서 구현
        """
        pass

    def close(self):
        """
        DB 연결을 닫는다.
        """
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed.")
            except pymysql.MySQLError as e:
                logger.error(f"Error closing database connection: {e}")
            finally:
                self.connection = None
