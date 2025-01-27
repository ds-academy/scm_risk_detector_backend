import os
import sys
import logging
import glob
import pandas as pd
from module.logger import get_logger, setup_global_logging
from module.utils import read_config, load_db_config_yaml
from module.data.database.stock_data_inserter import StockDataInserter

logger = get_logger(__name__)


def insert_stock_kor_main(config_path: str, db_params: dict):
    """
    KOR 주식 데이터 삽입 스크립트
    1) config_path -> kor_stock_price.yaml
    2) db_params -> DB 접속정보(dict)
    3) CSV 파일 로딩 -> insert_stock_price()
    """
    # 1) YAML config 로드
    config = read_config(config_path)
    data_pipelines = config["data_pipelines"]

    # 2) 경로, 종목 목록
    base_path = data_pipelines["base_path"]  # 예: "data/stocks/KOR"
    stocks_list = data_pipelines["stocks"]

    # 3) StockDataInserter 생성
    inserter = StockDataInserter(
        host=db_params["host"],
        user=db_params["user"],
        password=db_params["password"],
        db=db_params["database"],
        port=db_params["port"]
    )

    try:
        for stock in stocks_list:
            symbol = stock["symbol"]  # 예: "005930"
            folder_path = os.path.join(base_path, symbol)
            if not os.path.isdir(folder_path):
                logger.warning(f"No folder for {symbol} at {folder_path}")
                continue

            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            if not csv_files:
                logger.info(f"No CSV files found for {symbol}")
                continue

            for csvf in csv_files:
                df = pd.read_csv(csvf).drop_duplicates()
                # date 컬럼을 datetime 변환
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                # date가 NaT인 행 제거
                df.dropna(subset=["date"], inplace=True)

                if not df.empty:
                    # DB에 batch insert
                    inserter.insert_stock_price(symbol, df)
    finally:
        inserter.close()


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    sys.path.append(project_root)

    # 로깅 설정
    setup_global_logging(
        log_dir=os.path.join(project_root, "logs"),
        log_level=logging.INFO,
        file_level=logging.DEBUG,
        stream_level=logging.INFO,
    )

    logger.info("Starting insert_stock_kor script")

    # 1) DB config YAML 로드
    #    예: local_test_db_config.yaml or db_config.yaml
    db_config_path = os.path.join(
        project_root,
        "configs",
        "datasources",
        "local_test_db_config.yaml"
    )
    db_conf = load_db_config_yaml(db_config_path)

    # 2) kor_stock_price.yaml 경로
    stock_config_path = os.path.join(
        project_root,
        "configs",
        "datasources",
        "kor_scm_stock_price.yaml"
    )

    # 3) 메인 실행
    insert_stock_kor_main(stock_config_path, db_conf)
    logger.info("insert_stock_kor script completed.")
