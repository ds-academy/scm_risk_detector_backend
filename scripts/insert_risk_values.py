import os
import sys
import logging
import pandas as pd
from module.logger import get_logger, setup_global_logging
from module.utils import read_config, load_db_config_yaml
from module.data.database.risk_data_inserter import RiskDataInserter

logger = get_logger(__name__)


def insert_risk_values_main(config_path: str, db_params: dict):
    """
    1) config 로드 -> base_path (예: "data/stocks/KOR"), stocks 목록
    2) risk 저장 경로 -> project_root/data/risk/KOR/{symbol}/risk_values.csv
    3) CSV 로드 -> Inserter Upsert
    """

    # (A) config 로드
    config = read_config(config_path)
    data_pipelines = config["data_pipelines"]
    base_path = data_pipelines["base_path"]  # e.g. "data/stocks/KOR"
    stocks_list = data_pipelines["stocks"]  # e.g. [{"symbol":"005930"}, ...]

    country_str = os.path.basename(base_path)  # e.g. "KOR"

    # (B) DB 연결
    inserter = RiskDataInserter(
        host=db_params["host"],
        user=db_params["user"],
        password=db_params["password"],
        db=db_params["database"],
        port=db_params["port"]
    )

    try:
        # (C) project_root
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)

        # (D) 각 종목별 risk_values.csv 읽어서 Upsert
        for stock_info in stocks_list:
            symbol = stock_info["symbol"]  # e.g. "005930"
            risk_folder = os.path.join(project_root, "data", "risk", country_str, symbol)
            if not os.path.isdir(risk_folder):
                logger.info(f"[{symbol}] No risk folder: {risk_folder}")
                continue

            csv_file = os.path.join(risk_folder, "risk_values.csv")
            if not os.path.isfile(csv_file):
                logger.info(f"[{symbol}] No risk_values.csv found in: {risk_folder}")
                continue

            df_risk = pd.read_csv(csv_file)
            if df_risk.empty:
                logger.info(f"[{symbol}] risk_values.csv is empty.")
                continue

            # (E) CSV에 company_code 컬럼이 있는지 확인
            #     만약 "symbol"이라는 이름으로 되어 있다면 rename
            if "symbol" in df_risk.columns:
                df_risk.rename(columns={"symbol": "company_code"}, inplace=True)
            else:
                # CSV에서 이미 company_code로 저장했을 수도 있음
                if "company_code" not in df_risk.columns:
                    # 최후 수단: DF에 직접 추가
                    df_risk["company_code"] = symbol

            # (F) risk_score 소수 4자리로 맞춤
            df_risk["risk_score"] = df_risk["risk_score"].astype(float).round(4)

            # (G) Upsert
            inserter.insert_or_update_risk(df_risk)
            logger.info(f"[{symbol}] Upsert completed: {len(df_risk)} rows => RISK table.")

    finally:
        inserter.close()


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    sys.path.append(project_root)

    # 로깅
    setup_global_logging(
        log_dir=os.path.join(project_root, "logs"),
        log_level=logging.INFO,
        file_level=logging.DEBUG,
        stream_level=logging.INFO,
    )

    logger.info("Starting insert_risk_values script")

    # (1) DB config 로드 (예: db_config.yaml)
    db_config_path = os.path.join(project_root, "configs", "datasources", "db_config.yaml")
    db_conf = load_db_config_yaml(db_config_path)

    # (2) kor_scm_stock_price.yaml (예시)
    risk_config_path = os.path.join(
        project_root, "configs", "datasources", "kor_scm_stock_price.yaml"
    )

    # (3) main 실행
    insert_risk_values_main(risk_config_path, db_conf)
    logger.info("insert_risk_values script completed.")
