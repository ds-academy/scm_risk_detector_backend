import requests
import pandas as pd
from typing import Optional
from module.data.providers.core import DataProvider


class TwelveData(DataProvider):

    API_ADDRESS = "https://api.twelvedata.com/time_series"

    def __init__(
        self,
        api_key: str,
        symbol: str,
        interval: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        country: str = "US",
        exchange: Optional[str] = None,
        type: str = "stock",
    ):
        super().__init__()

        self.api_key = api_key
        self.symbol = symbol
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        self.country = country
        self.exchange = exchange
        self.type = type

    def get_data(
        self,
    ) -> Optional[pd.DataFrame]:

        params = {
            "apikey": self.api_key,
            "symbol": self.symbol,
            "interval": self.interval,
            "country": self.country,
            "type": self.type,
        }

        if self.start_date:
            params["start_date"] = self.start_date
        if self.end_date:
            params["end_date"] = self.end_date
        if self.exchange:
            params["exchange"] = self.exchange

        try:
            response = requests.get(self.API_ADDRESS, params=params)
            response.raise_for_status()
            data = response.json()

            if "values" in data:
                df = pd.DataFrame(data["values"])
                df["datetime"] = pd.to_datetime(df["datetime"])
                df = df.drop_duplicates(subset=["datetime"], keep="last")
                df = df.set_index("datetime")
                for col in ["open", "high", "low", "close", "volume"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                return df
            else:
                print(f"API 응답에 'values' 키가 없습니다. 응답: {data}")
                return None

        except requests.RequestException as e:
            print(f"API 호출 중 오류 발생: {e}")
            return None

    def ping(self) -> bool:
        pass
