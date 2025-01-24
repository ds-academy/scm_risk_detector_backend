import os
import yaml
import time
import pandas as pd
import concurrent.futures
import threading
import pytz
import importlib
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Callable
from module.data.providers.data_pipeline import ProviderDataPipeline, DataProvider
from module.logger import get_logger

logger = get_logger(__name__)

# 상수 정의
CONFIG_KEY_STRATEGY = "strategy"
CONFIG_KEY_ALGORITHM = "algorithm"
CONFIG_KEY_DATA_PIPELINES = "data_pipelines"
CONFIG_KEY_NAME = "name"

CONFIG_KEY_STOCKS = "stocks"
CONFIG_KEY_BASE_PATH = "base_path"
CONFIG_KEY_STOCKS_FILE = "stocks_file"


def find_project_root(current_path: str) -> str:
    logger.info(f"Searching for project root from: {current_path}")
    while True:
        if os.path.exists(os.path.join(current_path, ".git")):
            logger.info(f"Project root found: {current_path}")
            return current_path
        parent = os.path.dirname(current_path)
        if parent == current_path:
            logger.error("Project root not found")
            raise ValueError("Project root not found")
        current_path = parent


def read_config(config_path: str) -> Dict[str, Any]:
    logger.info(f"Reading config file: {config_path}")
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
        logger.debug("Config file loaded successfully")
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise FileNotFoundError(f"Config file not found: {config_path}")
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise ValueError(f"Error parsing YAML file: {e}")

    project_root = find_project_root(os.path.dirname(os.path.abspath(config_path)))

    # 새로운 구조로 config 재구성
    new_config = {
        CONFIG_KEY_STRATEGY: {},
        CONFIG_KEY_ALGORITHM: {},
        CONFIG_KEY_DATA_PIPELINES: {},
    }

    # Strategy 정보 처리
    if CONFIG_KEY_STRATEGY in config:
        new_config[CONFIG_KEY_STRATEGY] = config[CONFIG_KEY_STRATEGY]
        # Strategy 내의 data_pipelines 정보를 별도로 이동
        if CONFIG_KEY_DATA_PIPELINES in new_config[CONFIG_KEY_STRATEGY]:
            new_config[CONFIG_KEY_DATA_PIPELINES] = new_config[CONFIG_KEY_STRATEGY].pop(
                CONFIG_KEY_DATA_PIPELINES
            )

    # Algorithm 정보 처리
    if CONFIG_KEY_ALGORITHM in config:
        new_config[CONFIG_KEY_ALGORITHM] = config[CONFIG_KEY_ALGORITHM]

    # Data Pipelines 정보 처리
    if CONFIG_KEY_DATA_PIPELINES in config:
        new_config[CONFIG_KEY_DATA_PIPELINES] = config[CONFIG_KEY_DATA_PIPELINES]

    # Data Pipelines 정보가 없는 경우 처리
    if not new_config[CONFIG_KEY_DATA_PIPELINES]:
        logger.warning("No data pipeline configuration found")
        new_config[CONFIG_KEY_DATA_PIPELINES] = {}

    # base_path 설정
    if CONFIG_KEY_BASE_PATH in new_config[CONFIG_KEY_DATA_PIPELINES]:
        new_config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_BASE_PATH] = os.path.normpath(
            os.path.join(
                project_root,
                new_config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_BASE_PATH],
            )
        )
    else:
        new_config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_BASE_PATH] = os.path.join(
            project_root, "data"
        )

    # stocks_file 처리
    if CONFIG_KEY_STOCKS_FILE in new_config[CONFIG_KEY_DATA_PIPELINES]:
        stocks_file = new_config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_STOCKS_FILE]
        stocks_path = os.path.join(os.path.dirname(config_path), stocks_file)
        try:
            with open(stocks_path, "r", encoding="utf-8") as file:
                stocks_config = yaml.safe_load(file)
            new_config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_STOCKS] = stocks_config[
                CONFIG_KEY_STOCKS
            ]
        except FileNotFoundError:
            logger.warning(
                f"Stocks file '{stocks_file}' not found. Using stocks defined in main config."
            )
        except KeyError:
            logger.warning(
                f"Invalid structure in stocks file '{stocks_file}'. Using stocks defined in main config."
            )

    logger.info("Config processing completed")
    return new_config


def load_module(config: Dict, type_key: str):
    """
    주어진 type_key (예: CONFIG_KEY_DATA_PIPELINES) 하위에
    name, module 정보가 있어야 함
    """
    name = config[type_key]["name"]
    if "module" in config[type_key]:
        module_path = config[type_key]["module"]
    else:
        raise NotImplementedError("No 'module' key found in config")

    try:
        module = importlib.import_module(module_path)
        return getattr(module, name)
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to import {type_key} {name}: {e}")
        raise


def create_data_providers(config: Dict[str, Any]) -> List[DataProvider]:
    logger.info("Creating data providers")
    data_pipelines = config[CONFIG_KEY_DATA_PIPELINES]

    if CONFIG_KEY_NAME not in data_pipelines:
        logger.error(f"{CONFIG_KEY_NAME} not found in data_pipelines configuration")
        raise ValueError(f"{CONFIG_KEY_NAME} must be specified in the configuration")

    provider_name = data_pipelines[CONFIG_KEY_NAME]

    # 1) 'stocks' / 'companies' 분기
    if CONFIG_KEY_STOCKS in data_pipelines:
        items = data_pipelines[CONFIG_KEY_STOCKS]
    elif "companies" in data_pipelines:
        items = data_pipelines["companies"]
    else:
        logger.error("No 'stocks' or 'companies' key found in data_pipelines config")
        raise ValueError("Need either 'stocks' or 'companies' in data_pipelines config")

    # 모듈 로드
    try:
        provider_class = load_module(config, CONFIG_KEY_DATA_PIPELINES)
    except Exception as e:
        logger.error(f"Failed to load provider {provider_name}: {e}")
        raise

    logger.info(f"Using provider class: {provider_class.__name__}")

    # Provider별로 다른 파라미터 목록 결정
    if provider_name == "YahooFinance":
        # YahooFinance는 interval, period 등 사용
        param_keys = ["interval", "period", "start_date", "end_date",
                      "raise_errors", "keepna", "timeout"]
    elif provider_name == "NaverNews":
        param_keys = ["display", "start", "raise_errors", "timeout",
                      "start_date", "end_date"]
    elif provider_name == "FinanceDataReader":
        # FinanceDataReader는 period가 없고, interval, start_date, end_date만 사용
        # (constructor: __init__(self, symbol, interval="1D", start_date=None, end_date=None))
        param_keys = ["interval", "start_date", "end_date"]
    else:
        # 기본값(주식 파이프라인 가정) or 필요에 따라 추가
        param_keys = ["interval", "start_date", "end_date"]

    providers = []
    for item in items:
        if "symbol" in item:
            symbol_or_query = item["symbol"]
        elif "query" in item:
            symbol_or_query = item["query"]
        else:
            logger.error("Item has neither 'symbol' nor 'query'")
            continue

        provider_params = {}
        for k in param_keys:
            val = item.get(k, data_pipelines.get(k, None))
            if k == "end_date" and val == "TODAY":
                val = datetime.now().strftime("%Y-%m-%d")

            provider_params[k] = val

        provider = provider_class(symbol_or_query, **provider_params)
        logger.debug(f"Created provider for: {symbol_or_query} with params={provider_params}")
        providers.append(provider)

    logger.info(f"Created {len(providers)} data providers")
    return providers


def load_data(
        dp: ProviderDataPipeline, n_days_before: Optional[int] = None
) -> Optional[pd.DataFrame]:
    logger.info(f"Loading data for symbol: {dp.data_provider.symbol}")
    try:
        if n_days_before is not None:
            end_date = datetime.now(tz=pytz.UTC).replace(tzinfo=None)
            start_date = end_date - timedelta(days=n_days_before)
            logger.debug(f"Loading data from {start_date} to {end_date}")
            data = dp.get_data_range(start_date, end_date)
        else:
            logger.debug("Loading all available data")
            data = dp.get_all_data()

        if data.empty:
            logger.warning(f"No data found for symbol: {dp.data_provider.symbol}")
            return None
        logger.info(f"Successfully loaded data for symbol: {dp.data_provider.symbol}")
        return data
    except Exception as e:
        logger.error(f"Error loading data for symbol {dp.data_provider.symbol}: {e}")
        return None


def process_data(
        dp: ProviderDataPipeline, n_days_before: Optional[int] = None
) -> Optional[Dict[str, pd.DataFrame]]:
    symbol = dp.data_provider.symbol
    logger.info(f"Processing data for symbol: {symbol}")
    try:
        dp.update_to_latest()
        data = load_data(dp, n_days_before)
        if data is not None:
            logger.info(f"Loaded data for {symbol}:")
            logger.info(f"Shape: {data.shape}")
            logger.info(f"Date range: {data.index.min()} to {data.index.max()}")
            # 여기에 추가적인 데이터 처리 로직을 구현할 수 있습니다.
        return {symbol: data}
    except Exception as e:
        logger.error(f"Error processing data for symbol {dp.data_provider.symbol}: {e}")
        return None


def create_pipelines(config: Dict[str, Any]) -> List[ProviderDataPipeline]:
    logger.info("Creating data pipelines")
    providers = create_data_providers(config)
    base_path = config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_BASE_PATH]
    pipelines = []
    for provider in providers:
        symbol_base_path = os.path.join(base_path, provider.symbol)
        pipeline = ProviderDataPipeline(
            data_provider=provider, base_path=symbol_base_path
        )
        pipelines.append(pipeline)
        logger.debug(f"Created pipeline for symbol: {provider.symbol}")

    logger.info(f"Created {len(pipelines)} data pipelines")
    return pipelines


def parallel_process(
        func: Callable, items: List[Any], n_days_before: Optional[int] = None
) -> List[Dict[str, pd.DataFrame]]:
    logger.info("Starting parallel processing")
    results = []
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(32, os.cpu_count() + 4)
    ) as executor:
        futures = [executor.submit(func, item, n_days_before) for item in items]

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                logger.error(f"An error occurred during parallel processing: {e}")

    logger.info("All data processing completed.")
    logger.info(f"Successfully processed {len(results)} items.")

    return results


def run_data_pipeline(config: Dict[str, Any]):
    pipelines = create_pipelines(config)

    logger.info(f"Created {len(pipelines)} data pipelines")

    logger.info("Starting initial fetch for all stocks")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(pipelines)) as executor:
        futures = []
        for pipeline in pipelines:
            futures.append(
                executor.submit(
                    pipeline.fetch_and_save_realtime, threading.Event(), True
                )
            )

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logger.error(
                    f"Initial fetch generated an exception: {exc}", exc_info=True
                )

    logger.info("Initial fetch for all stocks completed.")

    logger.info("Starting continuous data update")
    stop_event = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(pipelines)) as executor:
        futures = []
        for pipeline in pipelines:
            futures.append(
                executor.submit(pipeline.fetch_and_save_realtime, stop_event)
            )

        try:
            while True:
                time.sleep(1)  # 메인 스레드가 CPU를 과도하게 사용하지 않도록 함
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt. Stopping all tasks...")
            stop_event.set()

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logger.error(
                    f"Continuous fetch generated an exception: {exc}", exc_info=True
                )

    logger.info("All tasks completed.")


def create_strategy(config: Dict[str, Any]):
    algorithm_config = config.get(CONFIG_KEY_ALGORITHM, {})
    strategy_config = config.get(CONFIG_KEY_STRATEGY, {})

    if not strategy_config:
        logger.warning("No strategy configuration found")
        return None

    try:
        strategy_class = load_module(config, CONFIG_KEY_STRATEGY)
    except Exception as e:
        logger.error(f"Failed to load strategy: {e}")
        return None

    # Create data pipelines
    data_pipelines = create_pipelines(config)  # List[DataPipelines]
    # Create algorithm
    algorithm = None
    if algorithm_config:
        try:
            algorithm_class = load_module(config, CONFIG_KEY_ALGORITHM)
            algorithm = algorithm_class(**algorithm_config["params"])
        except Exception as e:
            logger.error(f"Failed to load algorithm: {e}")

    strategy_params = strategy_config["params"]
    strategy_params["dps"] = data_pipelines
    if algorithm:
        strategy_params["algo"] = algorithm
    return strategy_class(**strategy_params)


def prepare_data(dp_result: List) -> pd.DataFrame:
    logger.info("Preparing data for strategy execution")

    aggregated_data = []
    for data in dp_result:
        for k, df in data.items():
            if df is None:
                logger.warning(f"데이터가 없습니다: {k}")
                continue
            if df.empty:
                logger.warning(f"빈 데이터프레임입니다: {k}")
                continue
            if "close" not in df.columns:
                logger.warning(f"'close' 컬럼이 없습니다: {k}")
                continue

            close_price = df["close"]
            if close_price.empty:
                logger.warning(f"'close' 가격 데이터가 비어 있습니다: {k}")
                continue

            close_price.name = k
            aggregated_data.append(close_price)
            logger.info(f"{k}: shape {close_price.shape}")

    # 중복된 인덱스를 제거합니다.
    for i, data in enumerate(aggregated_data):
        if data.index.duplicated().any():
            logger.warning(f"중복된 인덱스를 제거합니다: {data.name}")
            aggregated_data[i] = data[~data.index.duplicated(keep='first')]

    all_data = pd.concat(aggregated_data, axis=1)

    if not isinstance(all_data.index, pd.DatetimeIndex):
        all_data.index = pd.to_datetime(all_data.index)
        logger.info("인덱스를 datetime 형식으로 변환했습니다.")

    all_data = all_data.resample("1D").last().bfill().ffill()
    all_data = all_data.sort_index()
    return all_data


def create_symbol_mapper(configs: List[Dict]) -> Dict[str, str]:
    symbol_mapper = {}
    for config in configs:
        if "data_pipelines" in config and "stocks" in config["data_pipelines"]:
            data_info = config["data_pipelines"]["stocks"]
            for d in data_info:
                if "symbol" in d and "full_name" in d:
                    symbol_mapper[d["symbol"]] = d["full_name"]
    return symbol_mapper
