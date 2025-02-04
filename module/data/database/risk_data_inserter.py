import pymysql
import pandas as pd
from module.data.database.db_connector import DBConnector
from module.logger import get_logger

logger = get_logger(__name__)


class RiskDataInserter(DBConnector):
    """
    RISK 테이블 (COMPANY_CODE, MODEL_NAME, ANALYSIS_RESULT, TEST_DATE, PREDICT_DATE, RISK_SCORE) 삽입/업데이트 담당.
    FOREIGN KEY (COMPANY_CODE) REFERENCES COMPANY_META (COMPANY_CODE).
    """

    def select(self, where: str = None):
        pass

    def update(self, data: dict, where: str):
        pass

    def delete(self, where: str):
        pass

    def select_risk_rows(self, company_code: str, model_name: str, predict_date: str, test_date: str):
        """
        특정 (COMPANY_CODE, MODEL_NAME, PREDICT_DATE, TEST_DATE)에 해당하는 RISK 레코드를 조회.
        """
        try:
            with self.connection.cursor() as cursor:
                sql = """
                SELECT * FROM RISK
                 WHERE COMPANY_CODE=%s
                   AND MODEL_NAME=%s
                   AND PREDICT_DATE=%s
                   AND TEST_DATE=%s
                """
                cursor.execute(sql, (company_code, model_name, predict_date, test_date))
                return cursor.fetchall()
        except pymysql.MySQLError as e:
            logger.error(f"[RiskDataInserter] Error executing select_risk_rows: {e}")
            return None

    def insert_risk_row(self, row_data: dict):
        sql = """
        INSERT INTO RISK
        (COMPANY_CODE, MODEL_NAME, ANALYSIS_RESULT, TEST_DATE, PREDICT_DATE, RISK_SCORE)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (
                    row_data["company_code"],
                    row_data["model_name"],
                    row_data["analysis_result"],
                    row_data["test_date"],
                    row_data["predict_date"],
                    row_data["risk_score"]
                ))
            self.connection.commit()
            logger.debug(f"[RiskDataInserter] Inserted RISK row: {row_data}")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"[RiskDataInserter] Error inserting risk row: {e}")

    def update_risk_row(self, row_data: dict):
        sql = """
        UPDATE RISK
           SET ANALYSIS_RESULT=%s,
               RISK_SCORE=%s
         WHERE COMPANY_CODE=%s
           AND MODEL_NAME=%s
           AND TEST_DATE=%s
           AND PREDICT_DATE=%s
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (
                    row_data["analysis_result"],
                    row_data["risk_score"],
                    row_data["company_code"],
                    row_data["model_name"],
                    row_data["test_date"],
                    row_data["predict_date"],
                ))
            self.connection.commit()
            logger.debug(f"[RiskDataInserter] Updated RISK row: {row_data}")
        except pymysql.MySQLError as e:
            self.connection.rollback()
            logger.error(f"[RiskDataInserter] Error updating risk row: {e}")

    def insert_or_update_risk(self, df: pd.DataFrame):
        if df.empty:
            logger.info("[RiskDataInserter] Received empty DataFrame.")
            return

        required = {"company_code", "model_name", "analysis_result", "test_date", "predict_date", "risk_score"}
        missing = required - set(df.columns)
        if missing:
            logger.error(f"[RiskDataInserter] Missing columns in DataFrame: {missing}")
            return

        df["risk_score"] = df["risk_score"].fillna(0.0)
        df["risk_score"] = df["risk_score"].astype(float).round(4)

        for _, row in df.iterrows():
            company_code = str(row["company_code"])
            model_name = str(row["model_name"])
            analysis_result = str(row["analysis_result"])
            test_date = str(row["test_date"])  # "YYYY-MM-DD"
            predict_date = str(row["predict_date"])  # "YYYY-MM-DD"
            risk_score = float(row["risk_score"])  # 이제 NaN이 아닌 0.0 or 수치값

            existing = self.select_risk_rows(company_code, model_name, predict_date, test_date)
            row_data = {
                "company_code": company_code,
                "model_name": model_name,
                "analysis_result": analysis_result,
                "test_date": test_date,
                "predict_date": predict_date,
                "risk_score": risk_score
            }

            if existing and len(existing) > 0:
                self.update_risk_row(row_data)
            else:
                self.insert_risk_row(row_data)
