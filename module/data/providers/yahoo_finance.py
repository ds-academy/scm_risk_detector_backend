import yfinance as yf
import pandas as pd
from datetime import datetime
from typing import Optional
from module.data.providers.core import DataProvider
from module.logger import get_logger

logger = get_logger(__name__)


class YahooFinance(DataProvider):
    """
    get yahoo finance data
    default time zone : UTC
    """

    def __init__(
        self,
        symbol: str,
        interval: str,
        period: str,
        raise_errors: bool = True,
        keepna: bool = True,
        timeout: int = 100,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        self.symbol = symbol
        self.interval = interval
        self.period = period
        self.raise_errors = raise_errors
        self.keepna = keepna
        self.timeout = timeout

        if isinstance(start_date, datetime):
            start_date = self._format_date(start_date)
        if isinstance(end_date, datetime):
            end_date = self._format_date(end_date)

        super().__init__(start_date=start_date, end_date=end_date)
        logger.info(f"YahooFinance initialized for symbol: {symbol}")

    @staticmethod
    def _format_date(dt: Optional[datetime]) -> Optional[str]:
        return dt.date().isoformat() if dt else None

    def get_data(self) -> pd.DataFrame:
        logger.info(f"Fetching data for {self.symbol}")
        ticker = yf.Ticker(self.symbol)

        params = {
            "period": self.period,
            "interval": self.interval,
            "raise_errors": self.raise_errors,
            "keepna": self.keepna,
            "timeout": self.timeout,
            "prepost": True,
        }

        if self.start_date:
            params["start"] = self.start_date
            logger.debug(f"Start date set to {self.start_date}")
        if self.end_date:
            params["end"] = self.end_date
            logger.debug(f"End date set to {self.end_date}")

        try:
            logger.debug(f"Calling yfinance API with params: {params}")
            df = ticker.history(**params)

            if df.empty:
                logger.warning(f"No data found for {self.symbol}")
                return pd.DataFrame()

            logger.info(f"Data fetched successfully for {self.symbol}")
            logger.debug(f"Raw data shape: {df.shape}")

            df = df.reset_index()
            df = df.rename(columns={"Date": "datetime", "Volume": "volume"})
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.drop_duplicates(subset=["datetime"], keep="last")
            df["datetime"] = df["datetime"].dt.tz_convert("UTC")
            df = df.set_index("datetime")

            df.columns = df.columns.str.replace(" ", "_")
            df.columns = df.columns.str.lower()

            logger.debug(f"Processed data shape: {df.shape}")
            return df

        except Exception as e:
            logger.error(f"Error fetching data for {self.symbol}: {e}")
            return pd.DataFrame()

    def ping(self) -> bool:
        logger.info(f"Pinging Yahoo Finance for {self.symbol}")
        try:
            yf.Ticker(self.symbol).info
            logger.info(f"Ping successful for {self.symbol}")
            return True
        except Exception as e:
            logger.error(f"Ping failed for {self.symbol}: {e}")
            return False


if __name__ == "__main__":
    p =  YahooFinance(
        symbol="^TNX",
        interval="1d",
        period="max",
    )
    print(p.get_data().dropna().to_string())