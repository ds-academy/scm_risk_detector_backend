import os
import sys
import logging
import glob
import pandas as pd
from multiprocessing import Pool, cpu_count
from datetime import datetime
from module.analysis.ts.change_point_detection import calculate_risk_scores
from module.utils import read_config
from module.logger import get_logger, setup_global_logging

logger = get_logger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)


def process_csv_file(args):
    """
    Multiprocessing에서 병렬로 실행할 함수.
    CSV 파일 → DF 로드 → risk 계산 → 결과 DataFrame 반환
    """
    csv_file, symbol = args

    if not os.path.exists(csv_file):
        logger.warning(f"[{symbol}] CSV file not found: {csv_file}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(csv_file).drop_duplicates()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df.dropna(subset=["date"], inplace=True)

        if "close" not in df.columns or "volume" not in df.columns:
            logger.info(f"[{symbol}] Missing 'close' or 'volume' columns in {csv_file}. Skipped.")
            return pd.DataFrame()

        if df.empty:
            logger.info(f"[{symbol}] DataFrame is empty after date cleaning: {csv_file}")
            return pd.DataFrame()

        # 위험도 계산
        risk_df = calculate_risk_scores(df, symbol)
        if risk_df.empty:
            logger.info(f"[{symbol}] Returned empty risk_df from {csv_file}")
        else:
            logger.info(f"[{symbol}] Completed risk calculation for {csv_file} with {len(risk_df)} rows.")

        return risk_df

    except Exception as e:
        logger.warning(f"[{symbol}] Failed to process {csv_file}: {e}")
        return pd.DataFrame()


def main(config_path: str):
    # 1) config 로드
    config = read_config(config_path)
    data_pipelines = config["data_pipelines"]

    # 2) base_path 예: "data/stocks/KOR"
    base_path = data_pipelines["base_path"]
    stocks_list = data_pipelines["stocks"]

    # 예: base_path = "data/stocks/KOR" → country_str = "KOR"
    country_str = os.path.basename(base_path)

    tasks = []
    for stock in stocks_list:
        symbol = stock["symbol"]
        folder_path = os.path.join(base_path, symbol)
        if not os.path.isdir(folder_path):
            logger.warning(f"No folder for {symbol} at {folder_path}")
            continue

        csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
        if not csv_files:
            logger.info(f"No CSV files found for {symbol}")
            continue

        for csvf in csv_files:
            try:
                df_head = pd.read_csv(csvf, nrows=0)
                if "close" not in df_head.columns or "volume" not in df_head.columns:
                    logger.info(f"[{symbol}] {csvf} missing 'close'/'volume' columns. Skipped.")
                    continue
            except Exception as e:
                logger.warning(f"[{symbol}] Failed to read columns from {csvf} - {e}")
                continue

            tasks.append((csvf, symbol))

    logger.info(f"Total tasks to process: {len(tasks)}")

    if tasks:
        with Pool(processes=cpu_count()) as pool:
            results = pool.map(process_csv_file, tasks)
    else:
        logger.info("No tasks to process. Check if CSV files or columns are missing.")
        results = []

    # 종목별로 결과 DataFrame 합치기
    df_by_symbol = {}
    for res_df in results:
        if res_df.empty:
            continue
        sym = res_df["symbol"].iloc[0]
        if sym not in df_by_symbol:
            df_by_symbol[sym] = res_df.copy()
        else:
            df_by_symbol[sym] = pd.concat([df_by_symbol[sym], res_df], ignore_index=True)

    # 저장
    for sym, df_symbol in df_by_symbol.items():
        if df_symbol.empty:
            continue

        # 추가 컬럼
        df_symbol["model_name"] = "Binseg"
        df_symbol["analysis_result"] = "Completed"
        df_symbol["test_date"] = datetime.now().strftime("%Y-%m-%d")
        df_symbol["predict_date"] = df_symbol["date"]

        # date, predict_date를 YYYY-MM-DD로 포맷
        df_symbol["date"] = pd.to_datetime(df_symbol["date"]).dt.strftime("%Y-%m-%d")
        df_symbol["predict_date"] = pd.to_datetime(df_symbol["predict_date"]).dt.strftime("%Y-%m-%d")

        # symbol 컬럼 제거
        df_symbol.drop(columns=["symbol"], inplace=True)

        # risk_score = risk_value
        df_symbol["risk_score"] = df_symbol["risk_value"]

        # ex) "/Users/.../project_root/data/risk/KOR/005930"
        risk_folder = os.path.join(project_root, "data", "risk", country_str, sym)
        os.makedirs(risk_folder, exist_ok=True)

        out_file = os.path.join(risk_folder, "risk_values.csv")
        df_symbol.to_csv(out_file, index=False)
        logger.info(f"[{sym}] => {out_file} ({len(df_symbol)} rows)")


if __name__ == "__main__":
    setup_global_logging(
        log_dir=os.path.join(project_root, "logs"),
        log_level=logging.INFO,
        file_level=logging.DEBUG,
        stream_level=logging.INFO,
    )

    logger.info("Starting insert_stock_kor script")

    # config 파일 경로 (예시)
    config_path = os.path.join(
        project_root, "configs", "datasources", "kor_scm_stock_price.yaml"
    )

    main(config_path)
    logger.info("Script completed.")
