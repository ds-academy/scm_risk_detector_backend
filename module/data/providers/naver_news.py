import os
import requests
import pandas as pd
from typing import Optional
from newspaper import Article

from module.data.providers.core import DataProvider
from module.logger import get_logger

logger = get_logger(__name__)

# FIXME : 환경변수로 설정
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "oJ9H3ww68UJPMlQErEb_")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "gk7pKWpwA5")


class NaverNews(DataProvider):
    """
    네이버 뉴스 검색 API를 통해 뉴스 메타정보(제목, 링크, 날짜 등)를 DataFrame으로 제공.
    """

    def __init__(
            self,
            query: str,
            display: int = 10,
            start: int = 1,
            raise_errors: bool = True,
            timeout: int = 10,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
    ):
        super().__init__(start_date=start_date, end_date=end_date)
        self.query = query
        self.display = display
        self.start = start
        self.raise_errors = raise_errors
        self.timeout = timeout
        self.symbol = query
        logger.info(f"NaverNews initialized for query: {query}")

    def get_data(self) -> pd.DataFrame:
        """
        네이버 뉴스 API -> (title, originallink, link, description, pubDate 등) DataFrame
        """
        try:
            url = "https://openapi.naver.com/v1/search/news"
            headers = {
                "X-Naver-Client-Id": NAVER_CLIENT_ID,
                "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
            }
            params = {
                "query": self.query,
                "display": self.display,
                "start": self.start,
            }

            logger.info(f"Fetching news for query='{self.query}' (display={self.display})")
            response = requests.get(url, headers=headers, params=params, timeout=self.timeout)
            if response.status_code != 200:
                err_msg = f"Naver News API error code: {response.status_code}"
                logger.error(err_msg)
                if self.raise_errors:
                    raise RuntimeError(err_msg)
                return pd.DataFrame()

            data = response.json()
            items = data.get("items", [])
            if not items:
                logger.warning(f"No news items found for query='{self.query}'")
                return pd.DataFrame()

            df = pd.DataFrame(items)
            # pubDate를 datetime 변환(컬럼)
            if "pubDate" in df.columns:
                df["pubDate"] = pd.to_datetime(df["pubDate"], errors='coerce')
            df.drop_duplicates(subset=["originallink"], keep="last", inplace=True)
            df.dropna(subset=["pubDate"], inplace=True)

            df.sort_values("pubDate", inplace=True)

            logger.info(f"Fetched {len(df)} news for query='{self.query}'")
            return df.reset_index(drop=True)

        except Exception as e:
            logger.error(f"Error fetching news for {self.query}: {e}")
            if self.raise_errors:
                raise
            return pd.DataFrame()

    def ping(self) -> bool:
        try:
            test_df = self.get_data()
            return not test_df.empty
        except Exception as e:
            logger.error(f"Ping failed for NaverNews({self.query}): {e}")
            return False

    @staticmethod
    def fetch_content(url: str) -> dict:
        """
        article 본문 추출 (newspaper3k)
        """
        try:
            article = Article(url, language="ko")
            article.download()
            article.parse()
            return {
                "url": url,
                "title": article.title,
                "content": article.text
            }
        except Exception as e:
            return {"url": url, "error": str(e)}
