import time
import pandas as pd
import pytz
from typing import Optional
from datetime import datetime, timedelta
from module.data.providers.core import DataProvider
from module.data.providers.core import DataPipeline
from module.logger import get_logger

logger = get_logger(__name__)


class ProviderDataPipeline(DataPipeline):
    def __init__(
        self,
        data_provider: DataProvider,
        base_path: str,
        use_file_lock: bool = True,
        cache_days: int = 7,
        fetch_interval: int = 60,
        chunk_size: int = 10000,
    ):
        """
        실시간 데이터 파이프라인 초기화
        :param data_provider: 데이터 제공자 객체
        :param base_path: 데이터를 저장할 기본 경로
        :param use_file_lock: 파일 잠금 사용 여부
        :param cache_days: 메모리에 캐시할 날짜 수
        :param fetch_interval: 데이터 가져오기 간격 (초)
        :param chunk_size: 데이터를 저장할 청크 크기
        """
        super().__init__(data_provider, base_path, use_file_lock, cache_days)
        self.fetch_interval = fetch_interval
        self.chunk_size = chunk_size
        self._current_date = pd.Timestamp.now(tz=pytz.UTC).date()
        logger.info(
            f"ProviderDataPipeline initialized for {data_provider.symbol if data_provider else 'Unknown'}"
        )

    def fetch_data(self, **kwargs) -> pd.DataFrame:
        """새로운 데이터를 가져와 저장하고 캐시를 업데이트합니다."""
        if self.data_provider is None:
            logger.warning("Data provider is None, returning empty DataFrame")
            return pd.DataFrame()

        logger.info(f"Fetching new data for {self.data_provider.symbol}")
        self._cached_data = self.get_all_data()
        new_data = self.data_provider.get_data()

        if not new_data.empty:
            logger.debug(f"Received {len(new_data)} new rows of data")
            new_data["date"] = pd.to_datetime(new_data.index, utc=True)
            new_data = new_data.set_index("date")

            if not self._cached_data.empty:
                last_date = self._cached_data.index.max()
                new_data = new_data[new_data.index > last_date]
                logger.debug(
                    f"Filtered to {len(new_data)} new rows after last known date"
                )

            if not new_data.empty:
                self._save_data(new_data)
                self._cached_data = pd.concat(
                    [self._cached_data, new_data]
                ).sort_index()
                cutoff_date = pd.Timestamp.now(tz=pytz.UTC) - timedelta(
                    days=self.cache_days
                )
                self._cached_data = self._cached_data.loc[
                    self._cached_data.index >= cutoff_date
                ]
                logger.info(f"Updated cache with {len(new_data)} new rows")
        else:
            logger.info("No new data received")

        return new_data

    def fetch_and_save_realtime(self, stop_event, single_fetch=False):
        logger.info(f"Starting real-time fetch for {self.data_provider.symbol}")
        while not stop_event.is_set():
            current_date = pd.Timestamp.now(tz=pytz.UTC).date()
            if current_date != self._current_date:
                logger.info(f"Date changed. Saving data to new folder: {current_date}")
                self._current_date = current_date

            new_data = self.fetch_data()

            if not new_data.empty:
                self._save_data(new_data)
                logger.info(
                    f"{self.data_provider.symbol}: Saved {len(new_data)} new rows of data"
                )
            else:
                logger.info(f"{self.data_provider.symbol}: No new data")

            if single_fetch:
                logger.info("Single fetch completed, exiting loop")
                break

            logger.debug(f"Sleeping for {self.fetch_interval} seconds")
            time.sleep(self.fetch_interval)

    def fetch_start(self, **kwargs):
        """데이터 가져오기를 시작합니다."""
        logger.info(f"Starting data fetch for {self.data_provider.symbol}")
        self._cached_data = self.get_all_data()
        if self._cached_data.empty:
            logger.info("No existing data, fetching all data")
            self._cached_data = self.data_provider.get_data()
        else:
            logger.info(
                f"Existing data found, setting start date to {self._cached_data.index.max()}"
            )
            self.data_provider.start_date = self._cached_data.index.max()
        logger.info("Data fetch started successfully")
