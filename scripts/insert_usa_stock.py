import os
import sys
import logging
import glob
import yaml
import pandas as pd
from module.logger import get_logger, setup_global_logging
from module.utils import read_config, load_db_config_yaml
from module.data.database.stock_data_inserter import StockDataInserter

logger = get_logger(__name__)


def insert_stock_usa_main(config_path: str, db_params: dict):
    """
    USA 주식 데이터 삽입 스크립트
    """
    config = read_config(config_path)
    data_pipelines = config["data_pipelines"]
    base_path = data_pipelines["base_path"]  # e.g. "data/stocks/USA"
    stocks_list = data_pipelines["stocks"]

    # StockDataInserter 생성
    inserter = StockDataInserter(
        host=db_params["host"],
        user=db_params["user"],
        password=db_params["password"],
        db=db_params["database"],
        port=db_params["port"]
    )

    try:
        for stock in stocks_list:
            symbol = stock["symbol"]  # e.g. "AAPL"
            folder_path = os.path.join(base_path, symbol)
            if not os.path.isdir(folder_path):
                logger.warning(f"No folder for {symbol} at {folder_path}")
                continue

            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            for csvf in csv_files:
                df = pd.read_csv(csvf).drop_duplicates()
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df.dropna(subset=["date"], inplace=True)
                if not df.empty:
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

    logger.info("Starting insert_stock_usa script")

    # load configs
    db_config_path = os.path.join(project_root, "configs", "datasources", "db_config.yaml")
    db_conf = load_db_config_yaml(db_config_path)

    stock_config_path = os.path.join(project_root, "configs", "datasources", "usa_stock_price.yaml")

    # 2) insert_stock_usa_main() 호출
    insert_stock_usa_main(stock_config_path, db_conf)
    logger.info("insert_stock_usa script completed.")
