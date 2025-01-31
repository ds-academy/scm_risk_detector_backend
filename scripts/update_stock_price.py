# scripts/update_stock_price.py
import os
import sys
import logging
import glob
import pandas as pd

from module.logger import get_logger, setup_global_logging
from module.utils import read_config, load_db_config_yaml
from module.data.database.stock_data_inserter import StockDataInserter

logger = get_logger(__name__)


def almost_equal(a: float, b: float, tol=1e-4):
    return abs(a - b) <= tol


def update_stock_price_main(config_path: str, db_params: dict):
    """
    1) config에서 base_path, stocks 목록을 불러옴
    2) 각 stock별로 DB에서 last_date를 가져오고
    3) CSV 파일 읽어, (date > last_date) => INSERT, (date <= last_date & 값 다른 행) => UPDATE
    """
    config = read_config(config_path)
    data_pipelines = config["data_pipelines"]
    base_path = data_pipelines["base_path"]  # e.g. "data/stocks/KOR"
    stocks_list = data_pipelines["stocks"]  # [{ symbol: "005930", full_name: "삼성전자", ... }, ...]

    inserter = StockDataInserter(
        host=db_params["host"],
        user=db_params["user"],
        password=db_params["password"],
        db=db_params["database"],
        port=db_params["port"]
    )
    try:
        for stock_info in stocks_list:
            symbol = stock_info["symbol"]
            folder_path = os.path.join(base_path, symbol)
            if not os.path.isdir(folder_path):
                logger.warning(f"No folder for {symbol} at {folder_path}")
                continue

            # csv 파일들 모두 읽어 concat or 하나씩 처리
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            if not csv_files:
                logger.info(f"No csv files for {symbol}")
                continue

            # (옵션) 여러 CSV를 모두 concat
            df_all = []
            for csv_file in csv_files:
                df_part = pd.read_csv(csv_file)
                df_part.columns = df_part.columns.str.upper()
                df_part.drop_duplicates(inplace=True)

                df_all.append(df_part)
            df = pd.concat(df_all, ignore_index=True).drop_duplicates()

            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
            df["DATE"] = df["DATE"].dt.tz_localize(None)
            df.dropna(subset=["DATE"], inplace=True)

            # 1) DB에서 last_date 가져오기
            result = inserter.select(f"COMPANY_CODE='{symbol}' ORDER BY DATE DESC LIMIT 1")
            if result:
                last_date_in_db = result[0]["DATE"]
                # DB값도 혹시 tz가 있을 경우 제거
                if last_date_in_db.tzinfo is not None:
                    last_date_in_db = last_date_in_db.tz_localize(None)
            else:
                last_date_in_db = None

            df.sort_values("DATE", inplace=True)

            # 2) 신규 삽입 대상: if last_date_in_db is not None => filter date > last_date_in_db
            if last_date_in_db:
                df_new = df[df["DATE"] > last_date_in_db]
            else:
                df_new = df  # DB 비어있으면 전체가 insert 대상

            if not df_new.empty:
                logger.info(f"{symbol}: Found {len(df_new)} new rows to INSERT.")
                inserter.insert_stock_price(symbol, df_new)

            # 3) 기존 구간(= date <= last_date_in_db) 중 변경된 행 => UPDATE
            #    Step A) DB에서 (date <= last_date_in_db) 전체를 select
            #    Step B) compare with CSV
            if last_date_in_db:
                df_old = df[df["DATE"] <= last_date_in_db]
            else:
                # DB was empty => no old data to update
                df_old = pd.DataFrame()

            if not df_old.empty:
                # DB에서 old range를 select
                min_date = df_old["DATE"].min()
                max_date = df_old["DATE"].max()
                where_clause = f"COMPANY_CODE='{symbol}' AND DATE >= '{min_date}' AND DATE <= '{max_date}' ORDER BY DATE"
                db_rows = inserter.select(where_clause)
                # db_rows => list[dict], each dict: {COMPANY_CODE, DATE, OPEN, HIGH, LOW, CLOSE}
                # convert to DataFrame to compare easily
                if db_rows:
                    df_db = pd.DataFrame(db_rows)
                    df_db["DATE"] = pd.to_datetime(df_db["DATE"])

                    # join df_old and df_db on date
                    df_old_merged = pd.merge(
                        df_old,
                        df_db,
                        left_on="DATE",
                        right_on="DATE",
                        suffixes=("_csv", "_db"),
                        how="inner"
                    )
                    # => columns: [date_csv, open_csv, high_csv, ..., DATE_db, OPEN_db, HIGH_db, ... COMPANY_CODE_db]
                    # compare row by row
                    # e.g. "open_csv != OPEN_db", ...
                    for idx2, row2 in df_old_merged.iterrows():
                        open_csv = float(row2["OPEN_csv"])
                        open_db = float(row2["OPEN_db"])
                        high_csv = float(row2["HIGH_csv"])
                        high_db = float(row2["HIGH_db"])
                        low_csv = float(row2["LOW_csv"])
                        low_db = float(row2["LOW_db"])
                        close_csv = float(row2["CLOSE_csv"])
                        close_db = float(row2["CLOSE_db"])

                        date_val = row2["DATE"]  # or row2["DATE_db"]
                        # check if there's any difference
                        changed = not (
                                almost_equal(open_csv, open_db, tol=1e-4) and
                                almost_equal(high_csv, high_db, tol=1e-4) and
                                almost_equal(low_csv, low_db, tol=1e-4) and
                                almost_equal(close_csv, close_db, tol=1e-4)
                        )

                        if changed:
                            # do update
                            update_data = {
                                "OPEN": open_csv,
                                "HIGH": high_csv,
                                "LOW": low_csv,
                                "CLOSE": close_csv
                            }

                            # where
                            date_str = date_val.strftime("%Y-%m-%d %H:%M:%S")
                            where_u = f"COMPANY_CODE='{symbol}' AND DATE='{date_str}'"
                            inserter.update(update_data, where_u)
                            logger.info(f"Updated {symbol} {date_str} with new OHLC from CSV.")
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
        stream_level=logging.INFO,
    )

    logger.info("Starting update_stock_price script")

    # load configs
    db_config_path = os.path.join(project_root, "configs", "datasources", "local_test_db_config.yaml")
    db_conf = load_db_config_yaml(db_config_path)

    usa_stock_config_path = os.path.join(
        project_root, "configs", "datasources", "usa_stock_price.yaml"
    )
    kor_stock_config_path = os.path.join(
        project_root, "configs", "datasources", "kor_stock_price.yaml"
    )

    # update stock price
    update_stock_price_main(usa_stock_config_path, db_conf)
    update_stock_price_main(kor_stock_config_path, db_conf)
    logger.info("Script completed")
