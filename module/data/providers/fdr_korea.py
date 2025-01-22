import FinanceDataReader as fdr
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta
from module.data.providers.core import DataProvider
from module.logger import get_logger

logger = get_logger(__name__)


class FinanceDataReader(DataProvider):
    """
    get Korea market data
    default time zone : UTC
    """

    def __init__(
        self,
        symbol: str,
        interval: str = "1D",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        self.symbol = symbol
        self.interval = interval
        super().__init__(start_date=start_date, end_date=end_date)
        logger.info(f"FinanceDataReader initialized for symbol: {symbol}")

    def get_data(self) -> pd.DataFrame:
        logger.info(f"Fetching data for {self.symbol}")
        params = {
            "symbol": self.symbol,
        }

        if self.start_date:
            params["start"] = self.start_date
            logger.debug(f"Start date set to {self.start_date}")
        if self.end_date:
            params["end"] = self.end_date
            logger.debug(f"End date set to {self.end_date}")

        try:
            logger.debug(f"Calling FinanceDataReader with params: {params}")
            df = fdr.DataReader(**params)

            if df.empty:
                logger.warning(f"No data found for {self.symbol}")
                return pd.DataFrame()

            logger.info(f"Data fetched successfully for {self.symbol}")
            logger.debug(f"Raw data shape: {df.shape}")

            df = df.reset_index()
            df.columns = df.columns.str.lower()
            df = df.rename(columns={"date": "datetime"})
            df = df.set_index("datetime")

            # 1. tz information이 없기 때문에 Asia/Seoul 기준으로 tz 셋업
            # 2. 그 다음 change UTC
            df.index = df.index.tz_localize("Asia/Seoul")
            df.index = df.index.tz_convert("UTC")

            logger.debug(f"Processed data shape: {df.shape}")
            return df

        except Exception as e:
            logger.error(f"Error fetching data for {self.symbol}: {e}")
            return pd.DataFrame()

    def ping(self) -> bool:
        logger.info(f"Pinging FinanceDataReader for {self.symbol}")
        try:
            end_date = self.end_date or datetime.now().strftime("%Y-%m-%d")
            start_date = (
                datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=7)
            ).strftime("%Y-%m-%d")
            logger.debug(f"Ping date range: {start_date} to {end_date}")
            df = fdr.DataReader(self.symbol, start=start_date, end=end_date)
            success = not df.empty
            logger.info(
                f"Ping {'successful' if success else 'failed'} for {self.symbol}"
            )
            return success
        except Exception as e:
            logger.error(f"Ping failed for {self.symbol}: {e}")
            return False
