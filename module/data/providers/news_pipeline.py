import os
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from module.data.providers.data_pipeline import ProviderDataPipeline
from module.data.providers.naver_news import NaverNews
from module.logger import get_logger

logger = get_logger(__name__)


class NewsDataPipeline(ProviderDataPipeline):
    """
    뉴스 전용 파이프라인.

    - 저장되는 데이터
        (1) news_link.csv: 기사 메타 정보(ID 인덱스, pubDate, title, originallink, link, description)
        (2) contents.json: { "0": { "pubDate":..., "title":..., "content":... }, ... }
    """

    def fetch_data(self) -> pd.DataFrame:
        """
        1) NaverNews.get_data() -> 기사 메타정보 DF
        2) news_link.csv 있다면 로드 -> concat 후 originallink 기준 중복 제거
        3) CSV에 index=True로 저장 -> ID(정수) 열 생성
        """
        if not isinstance(self.data_provider, NaverNews):
            logger.warning("Invalid data_provider. Expected NaverNews.")
            return pd.DataFrame()

        logger.info(f"Fetching news (meta) from Naver API for query='{self.data_provider.query}'")
        df_new = self.data_provider.get_data()
        if df_new.empty:
            logger.info("No new news found from Naver API.")
            return df_new

        csv_path = os.path.join(self.base_path, "news_link.csv")
        if os.path.exists(csv_path):
            old_df = pd.read_csv(csv_path, index_col=0)  # index_col=0 -> 첫 열(정수 ID)
            combined = pd.concat([old_df, df_new], ignore_index=True)
            combined.drop_duplicates(subset=["originallink"], keep="last", inplace=True)
        else:
            combined = df_new

        # CSV 저장 -> 기사 ID = 인덱스
        combined.to_csv(csv_path, index=True)
        logger.info(f"Saved total {len(combined)} rows (meta) to {csv_path}")
        return df_new

    def fetch_article_content(self) -> None:
        """
        (1) news_link.csv 로드
        (2) content가 없는 기사만 -> newspaper3k -> contents.json에 저장
        (3) CSV는 meta만 유지하고, content는 저장하지 않음
        """
        csv_path = os.path.join(self.base_path, "news_link.csv")
        if not os.path.exists(csv_path):
            logger.warning(f"{csv_path} not found. No meta to fetch content.")
            return

        df_all = pd.read_csv(csv_path, index_col=0)

        # content 컬럼이 CSV에 없더라도 상관없음 -> 여기서 임시로 관리만
        if "content" not in df_all.columns:
            df_all["content"] = None  # 임시

        # content가 NaN/""인 기사들만
        df_todo = df_all[df_all["content"].isnull() | (df_all["content"] == "")]
        if df_todo.empty:
            logger.info("All articles already have content (or none to fetch).")
            return

        urls = df_todo["originallink"].tolist()
        ids = df_todo.index.tolist()

        results = []
        with ThreadPoolExecutor() as executor:
            future_map = {
                executor.submit(NaverNews.fetch_content, url): article_id
                for article_id, url in zip(ids, urls)
            }
            for future in as_completed(future_map):
                article_id = future_map[future]
                try:
                    result = future.result()  # {"url":..., "title":..., "content":..., "error":...}
                    result["id"] = article_id
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error fetching content for ID={article_id}: {e}")

        # 기존 contents.json 로드 (있으면)
        json_path = os.path.join(self.base_path, "contents.json")
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                contents_data = json.load(f)
        else:
            contents_data = {}

        # results -> contents.json에만 저장, CSV에는 'content' 컬럼 제거
        for r in results:
            article_id = str(r["id"])
            pub_date = str(df_all.at[r["id"], "pubDate"]) if "pubDate" in df_all.columns else ""
            title = df_all.at[r["id"], "title"] if "title" in df_all.columns else ""

            if "error" not in r:
                # 실제 본문
                contents_data[article_id] = {
                    "pubDate": pub_date,
                    "title": title,
                    "content": r["content"]
                }
                # df_all.at[r["id"], "content"] = r["content"]  # <-- CSV에는 최종적으로 저장 X
            else:
                contents_data[article_id] = {
                    "pubDate": pub_date,
                    "title": title,
                    "content": "",
                    "error": r["error"]
                }
                # df_all.at[r["id"], "content"] = ""

        # 최종 JSON 덮어쓰기
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(contents_data, f, ensure_ascii=False, indent=2)

        # CSV에는 content 컬럼 제거 -> meta 정보만 남김
        if "content" in df_all.columns:
            df_all.drop(columns=["content"], inplace=True)

        # 기존 CSV 갱신 (메타정보만)
        df_all.to_csv(csv_path, index=True)
        logger.info(f"Fetched content for {len(results)} articles. JSON and CSV updated.\n"
                    f" -> {csv_path} (meta only), {json_path} (contents)")

    # 나머지 DataPipeline 오버라이드 메서드는 생략 or pass
    def _save_data(self, data: pd.DataFrame):
        pass

    def get_all_data(self) -> pd.DataFrame:
        # 필요 시 news_link.csv 전체를 로드 (메타 정보)
        csv_path = os.path.join(self.base_path, "news_link.csv")
        if not os.path.exists(csv_path):
            logger.warning(f"{csv_path} not found.")
            return pd.DataFrame()
        return pd.read_csv(csv_path, index_col=0)
