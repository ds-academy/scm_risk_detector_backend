import os
import sys
import logging
from module.logger import get_logger, setup_global_logging
from module.utils import read_config, load_db_config_yaml
from module.data.database.company_meta_inserter import CompanyMetaInserter

logger = get_logger(__name__)


def insert_company_meta_from_config(config_path: str, db_params: dict, country_code: str):
    """
    1) config_path => read_config()
    2) data_pipelines.stocks => for each stock, insert or update
    3) country_code => "USA" or "KOR"
    """
    config = read_config(config_path)
    data_pipelines = config["data_pipelines"]
    stocks_list = data_pipelines["stocks"]  # list of {symbol, full_name, exchange, ...}

    inserter = CompanyMetaInserter(
        host=db_params["host"],
        user=db_params["user"],
        password=db_params["password"],
        db=db_params["database"],
        port=db_params["port"]
    )

    try:
        for stock in stocks_list:
            symbol = stock["symbol"]  # e.g. "AAPL"
            name = stock.get("full_name", "")  # e.g. "Apple Inc."
            # exch = stock.get("exchange", "")  # e.g. "NASDAQ"
            sector = stock.get("sector", "")  # e.g. "Technology"
            # prepare dict for insert
            data = {
                "COMPANY_CODE": symbol,
                "COMPANY_NAME": name,
                "COUNTRY": country_code,  # "USA" or "KOR"
                "SECTOR": sector  # store exchange in SECTOR
            }
            # 먼저 존재 여부 확인
            rows = inserter.select(f"COMPANY_CODE='{symbol}'")
            if rows:
                logger.info(f"{symbol} already exists in COMPANY_META.")
                # update if needed
                # e.g. if we want to update new info:
                # inserter.update({"COMPANY_NAME": name, ...}, f"COMPANY_CODE='{symbol}'")
            else:
                # insert
                inserter.insert(data)
    finally:
        inserter.close()


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    sys.path.append(project_root)

    setup_global_logging(
        log_dir=os.path.join(project_root, "logs"),
        log_level=logging.INFO,
        file_level=logging.DEBUG,
        stream_level=logging.INFO
    )

    logger.info("Starting insert_company_meta script")

    # (1) Database config
    db_config_path = os.path.join(project_root, "configs", "datasources", "local_test_db_config.yaml")
    db_conf = load_db_config_yaml(db_config_path)

    # (2) USA config
    # usa_config_path = os.path.join(
    #     project_root, "configs", "datasources", "usa_stock_price.yaml"
    # )
    # insert_company_meta_from_config(usa_config_path, db_conf, "USA")

    # 2) KOR config
    kor_config_path = os.path.join(
        project_root, "configs", "datasources", "kor_scm_stock_price.yaml"
    )
    insert_company_meta_from_config(kor_config_path, db_conf, "KOR")

    logger.info("All done. Check company_meta table.")
