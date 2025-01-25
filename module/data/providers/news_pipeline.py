import os
import json
import pandas as pd
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from module.data.providers.data_pipeline import ProviderDataPipeline
from module.data.providers.naver_news import NaverNews
from module.analysis.llm.chat_gpt import GPTModel
from module.logger import get_logger

logger = get_logger(__name__)

KEY_MAP = {
    "Sentiment": "sentiment",
    "PositiveKeywords": "positiveKeywords",
    "NegativeKeywords": "negativeKeywords",
    "CompanyName": "companyName",
    "CompanySector": "companySector"
}


class NewsDataPipeline(ProviderDataPipeline):
    """
    뉴스 전용 파이프라인.

    - news_link.csv: 기사 메타(ID 인덱스, pubDate, title, originallink, link, description) - pubDate 오름차순 정렬
    - contents.json:
       {
         "0": { "pubDate":..., "title":..., "originallink":..., "link":..., "content":... },
         ...
       }
    """

    def fetch_data(self) -> pd.DataFrame:
        """
        1) NaverNews.get_data() -> 기사 메타정보 DF (title, originallink, link, description, pubDate)
        2) news_link.csv 있다면 로드 -> concat -> originallink 중복 제거
        3) pubDate 오름차순 정렬 → 0부터 정수 인덱스 → CSV에 index=True로 저장
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
            old_df = pd.read_csv(csv_path, index_col=0)
            combined = pd.concat([old_df, df_new], ignore_index=True)
            combined.drop_duplicates(subset=["originallink"], keep="last", inplace=True)
        else:
            combined = df_new

        # pubDate 오름차순 정렬
        if "pubDate" in combined.columns:
            combined["pubDate"] = pd.to_datetime(combined["pubDate"], errors="coerce")
            combined.sort_values("pubDate", inplace=True)

        # 정수 인덱스 재부여 및 저장
        combined.reset_index(drop=True, inplace=True)
        combined.to_csv(csv_path, index=True)

        logger.info(f"Saved total {len(combined)} rows (meta) to {csv_path} (sorted by pubDate)")
        return df_new

    def fetch_article_content(self) -> None:
        """
        (1) news_link.csv 로드 (이미 pubDate 정렬 + ID 인덱스)
        (2) content 없는 기사만 -> newspaper3k -> contents.json
        (3) contents.json도 ID 오름차순으로 정렬 후 저장
        (4) CSV에 content 칼럼은 저장 X
        """
        csv_path = os.path.join(self.base_path, "news_link.csv")
        if not os.path.exists(csv_path):
            logger.warning(f"{csv_path} not found. No meta to fetch content.")
            return

        # 메타 로드 (ID 인덱스)
        df_all = pd.read_csv(csv_path, index_col=0)
        # df_all는 이미 pubDate 오름차순 + ID(0..N) 순

        # 임시 content 컬럼(존재 안 할 수 있음)
        if "content" not in df_all.columns:
            df_all["content"] = None

        # content가 없는 기사만
        df_todo = df_all[df_all["content"].isnull() | (df_all["content"] == "")]
        if df_todo.empty:
            logger.info("All articles already have content or none to fetch.")
            return

        # originallink로 파싱
        urls = df_todo["originallink"].tolist()
        ids = df_todo.index.tolist()  # 정수 ID

        results = []
        with ThreadPoolExecutor() as executor:
            future_map = {
                executor.submit(NaverNews.fetch_content, url): article_id
                for article_id, url in zip(ids, urls)
            }
            for future in as_completed(future_map):
                article_id = future_map[future]
                try:
                    result = future.result()  # {"url":..., "content":..., "error":...}
                    result["id"] = article_id
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error fetching content for ID={article_id}: {e}")

        # 기존 contents.json 로드
        json_path = os.path.join(self.base_path, "contents.json")
        if os.path.exists(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                contents_data = json.load(f)
        else:
            contents_data = {}

        # 결과 -> contents_data 반영
        for r in results:
            article_id = str(r["id"])  # JSON 키 = 문자열
            pub_date = str(df_all.at[r["id"], "pubDate"]) if "pubDate" in df_all.columns else ""
            title = df_all.at[r["id"], "title"] if "title" in df_all.columns else ""
            origin_link = df_all.at[r["id"], "originallink"] if "originallink" in df_all.columns else ""
            link = df_all.at[r["id"], "link"] if "link" in df_all.columns else ""

            if "error" not in r:
                contents_data[article_id] = {
                    "pubDate": pub_date,
                    "title": title,
                    "originallink": origin_link,
                    "link": link,
                    "content": r["content"]
                }
            else:
                contents_data[article_id] = {
                    "pubDate": pub_date,
                    "title": title,
                    "originallink": origin_link,
                    "link": link,
                    "content": "",
                    "error": r["error"]
                }

        # (C) contents.json을 ID(정수) 순서대로 정렬
        #  -> 키가 "0","1","2"... 이므로, int 변환 후 정렬
        sorted_contents_items = sorted(contents_data.items(), key=lambda x: int(x[0]))
        sorted_contents_data = dict(sorted_contents_items)

        # 덮어쓰기
        with open(json_path, "w", encoding="utf-8") as f:
            # sort_keys=False: 우리가 위에서 ID 순서 정렬했으므로 안 써도 됨
            json.dump(sorted_contents_data, f, ensure_ascii=False, indent=2)

        # CSV에는 content 칼럼 제거, 메타만
        if "content" in df_all.columns:
            df_all.drop(columns=["content"], inplace=True)

        df_all.to_csv(csv_path, index=True)
        logger.info(
            f"Fetched content for {len(results)} articles. JSON and CSV updated.\n"
            f" -> {csv_path} (meta only, pubDate-sorted), {json_path} (contents, ID-sorted)"
        )

    def analyze_contents_with_gpt(self, api_key: str, model_id: str = "gpt-4o-mini", max_workers: int = 5):
        """
        수집된 뉴스 본문(contents.json)에 대해 감성 분석 -> report.json 저장.
        """
        contents_path = os.path.join(self.base_path, "contents.json")
        if not os.path.exists(contents_path):
            logger.warning(f"contents.json not found at {contents_path}, nothing to analyze.")
            return

        with open(contents_path, "r", encoding="utf-8") as f:
            contents_data: Dict[str, Dict[str, Any]] = json.load(f)

        if not contents_data:
            logger.info("contents.json is empty, no articles to analyze.")
            return

        logger.info(f"Starting GPT sentiment analysis with model={model_id}")
        gpt_model = GPTModel(api_key=api_key, model_id=model_id)

        report_data = {}

        items = list(contents_data.items())  # [(id_str, {pubDate, title, content, ...}), ...]
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {}
            for article_id_str, info in items:
                content_text = info.get("content", "")
                if not content_text:
                    continue
                future = executor.submit(
                    self._analyze_article_with_gpt, gpt_model, content_text, article_id_str
                )
                future_map[future] = article_id_str

            for future in as_completed(future_map):
                article_id_str = future_map[future]
                try:
                    gpt_result = future.result()  # GPT 결과 (dict)
                    results.append((article_id_str, gpt_result))
                except Exception as e:
                    logger.error(f"Error analyzing article ID={article_id_str}: {e}")

        # GPT 결과에 pubDate, title, originallink, link 추가 + 키 rename
        for article_id_str, analysis_dict in results:
            # contents.json에서 meta 정보 가져오기
            info = contents_data.get(article_id_str, {})
            pub_date = info.get("pubDate", "")
            title = info.get("title", "")
            origin_link = info.get("originallink", "")
            link = info.get("link", "")

            # 1) GPT가 준 키를 소문자 규칙으로 rename
            analysis_dict_renamed = self._rename_gpt_keys(analysis_dict)

            # 2) 기사 메타 필드 추가
            analysis_dict_renamed["pubDate"] = pub_date
            analysis_dict_renamed["title"] = title
            analysis_dict_renamed["originallink"] = origin_link
            analysis_dict_renamed["link"] = link

            report_data[article_id_str] = analysis_dict_renamed

        # ID 순 정렬
        sorted_report = dict(sorted(report_data.items(), key=lambda x: int(x[0])))

        # 저장
        report_path = os.path.join(self.base_path, "report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(sorted_report, f, ensure_ascii=False, indent=2)

        logger.info(f"Sentiment analysis completed. Saved report to {report_path}")

    def _analyze_article_with_gpt(self, gpt_model: GPTModel, content_text: str, article_id_str: str) -> Dict[str, Any]:
        instruction = (
            f"다음은 뉴스 기사 본문입니다:\n\n{content_text}\n\n"
            "위 문서를 분석해서 JSON 형식의 감성 분석 결과를 반환해주세요."
        )

        response_str = gpt_model.generate(instruction)
        logger.debug(f"GPT raw response for article {article_id_str}:\n{response_str}")

        try:
            analysis_dict = json.loads(response_str)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse GPT response as JSON for ID={article_id_str}. "
                           f"Raw response: {response_str[:200]}...")
            analysis_dict = {"error": "Invalid JSON from GPT", "raw_response": response_str}
        return analysis_dict

    def _rename_gpt_keys(self, analysis_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        GPT 반환 JSON의 키를 소문자/스네이크케이스 등으로 통일.
        (예) "Sentiment" -> "sentiment", "PositiveKeywords" -> "positiveKeywords", ...
        """
        new_dict = {}
        for old_key, value in analysis_dict.items():
            if old_key in KEY_MAP:
                new_key = KEY_MAP[old_key]  # e.g. "Sentiment" -> "sentiment"
            else:
                new_key = old_key  # 그 외 키는 그대로 둠
            new_dict[new_key] = value
        return new_dict

    # 필요 없는 메서드 오버라이드
    def _save_data(self, data: pd.DataFrame):
        pass

    def get_all_data(self) -> pd.DataFrame:
        """ news_link.csv 전체(메타 정보) 로드 """
        csv_path = os.path.join(self.base_path, "news_link.csv")
        if not os.path.exists(csv_path):
            logger.warning(f"{csv_path} not found.")
            return pd.DataFrame()
        return pd.read_csv(csv_path, index_col=0)
