from db_connector import DBConnector
import pymysql

class AlarmingDB(DBConnector):

    def select(self, where: str = None):
        """
        Alarming 테이블에서 모든 데이터를 조회하고 반환.
        
        :param where: 선택적 조건 문자열
        :return: 조회된 데이터의 리스트
        """
        try:
            with self.db_engine.cursor() as cursor:
                sql = "SELECT * FROM Alarming"
                if where:
                    sql += f" WHERE {where}"
                cursor.execute(sql)
                result = cursor.fetchall()
                return result
        except pymysql.MySQLError as e:
            print(f"Error executing select: {e}")
            return None

    def insert(self, data: dict):
        """
        Alarming 테이블에 데이터를 삽입.
        
        :param data: 삽입할 데이터 딕셔너리
        """
        try:
            with self.db_engine.cursor() as cursor:
                columns = ', '.join(data.keys())
                values = ', '.join(['%s'] * len(data))
                sql = f"INSERT INTO Alarming ({columns}) VALUES ({values});"
                cursor.execute(sql, tuple(data.values()))
                self.db_engine.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing insert: {e}")

    def update(self, data: dict, where: str):
        """
        Alarming 테이블의 특정 조건에 맞는 데이터를 업데이트.
        
        :param data: 업데이트할 데이터 딕셔너리
        :param where: 업데이트 조건 문자열
        """
        try:
            with self.db_engine.cursor() as cursor:
                set_clause = ', '.join([f"{key}=%s" for key in data.keys()])
                sql = f"UPDATE Alarming SET {set_clause} WHERE {where};"
                cursor.execute(sql, tuple(data.values()))
                self.db_engine.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing update: {e}")

    def delete(self, where: str):
        """
        Alarming 테이블의 특정 조건에 맞는 데이터를 삭제.
        
        :param where: 삭제 조건 문자열
        """
        try:
            with self.db_engine.cursor() as cursor:
                sql = f"DELETE FROM Alarming WHERE {where};"
                cursor.execute(sql)
                self.db_engine.commit()
        except pymysql.MySQLError as e:
            print(f"Error executing delete: {e}")
