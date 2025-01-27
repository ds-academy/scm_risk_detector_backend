import os
import sys
import logging
import pandas as pd
import json
from module.logger import get_logger, setup_global_logging
from module.utils import read_config, load_db_config_yaml
from module.data.database.news_data_inserter import NewsDataInserter

logger = get_logger(__name__)


def insert_news_main_core(config_path: str, db_params: dict):
    """
    1) 각 company_code 폴더의 news_link.csv/contents.json/report.json 읽어옴
    2) DB에 이미 있는 뉴스(NEWS_API) 제외 → 새 데이터만 INSERT
    3) NEWS_MAIN → NEWS_COMPANY → (option) NEWS_SENTIMENT
    """

    config = read_config(config_path)
    data_pipelines = config["data_pipelines"]
    base_path = data_pipelines["base_path"]  # e.g. "data/news"
    companies_list = data_pipelines["companies"]

    inserter = NewsDataInserter(
        host=db_params["host"],
        user=db_params["user"],
        password=db_params["password"],
        db=db_params["database"],
        port=db_params["port"]
    )
    try:
        for comp in companies_list:
            company_name = comp["full_name"]
            company_code = comp.get("symbol")
            folder_path = os.path.join(base_path, company_code)  # e.g. "data/news/005930"

            csv_path = os.path.join(folder_path, "news_link.csv")
            if not os.path.exists(csv_path):
                logger.warning(f"{csv_path} not found. skip {company_name}")
                continue

            df = pd.read_csv(csv_path, index_col=0).drop_duplicates()
            if "pubDate" in df.columns:
                df["pubDate"] = pd.to_datetime(df["pubDate"], errors="coerce")

            # 1) 로컬 JSON들 로드
            contents_path = os.path.join(folder_path, "contents.json")
            contents_data = {}
            if os.path.exists(contents_path):
                with open(contents_path, "r", encoding="utf-8") as f:
                    contents_data = json.load(f)

            report_path = os.path.join(folder_path, "report.json")
            report_data = {}
            if os.path.exists(report_path):
                with open(report_path, "r", encoding="utf-8") as f:
                    report_data = json.load(f)

            # 2) 각 행(뉴스) 순회
            for idx, row in df.iterrows():
                pub_date = row.get("pubDate")
                if pd.notna(pub_date) and hasattr(pub_date, "to_pydatetime"):
                    pub_date = pub_date.to_pydatetime()
                else:
                    pub_date = None

                title = row.get("title", "")
                originallink = row.get("originallink", "")
                link = row.get("link", "")
                news_api = originallink or link  # 유니크 key
                source = "Naver"  # or row.get("source", "Naver")

                # 2-1) **DB 중복 여부 체크** → NEWS_MAIN에 이미 해당 NEWS_API가 있는가?
                q_check = f"NEWS_API='{news_api}'"
                existing = inserter.select(q_check)  # SELECT * FROM NEWS_MAIN WHERE NEWS_API='..'
                if existing:
                    # 이미 DB에 존재 → Skip
                    logger.debug(f"Skip already exists => news_api={news_api}")
                    continue

                # 2-2) 기사 본문/감성분석
                content = ""
                if str(idx) in contents_data:
                    content = contents_data[str(idx)].get("content", "")

                # report.json
                sentiment_dict = report_data.get(str(idx), {})
                # e.g. { "sentiment": "0.75", "positiveKeywords": [...], ... }
                try:
                    sentiment_val = float(sentiment_dict.get("sentiment", 0.0))
                except ValueError:
                    sentiment_val = 0.0
                pos_list = sentiment_dict.get("positiveKeywords", [])
                neg_list = sentiment_dict.get("negativeKeywords", [])
                pos_str = ", ".join(pos_list) if pos_list else ""
                neg_str = ", ".join(neg_list) if neg_list else ""
                pub_date_sent = sentiment_dict.get("pubDate", None)
                if pub_date_sent:
                    try:
                        pub_date_sent = pd.to_datetime(pub_date_sent, errors="coerce")
                        if pd.notna(pub_date_sent) and hasattr(pub_date_sent, "to_pydatetime"):
                            pub_date_sent = pub_date_sent.to_pydatetime()
                    except:
                        pub_date_sent = pub_date

                # 3) Insert NEWS_MAIN
                new_id = inserter.insert_news_main(
                    title=title,
                    content=content,
                    pub_date=pub_date,
                    source=source,
                    news_api=news_api
                )

                # 4) Insert NEWS_COMPANY
                if company_code:
                    inserter.insert_news_company(new_id, company_code)

                # 5) Insert NEWS_SENTIMENT
                inserter.insert_news_sentiment(
                    news_id=new_id,
                    sentiment_val=sentiment_val,
                    pos_str=pos_str,
                    neg_str=neg_str,
                    pub_date=(pub_date_sent or pub_date)
                )

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

    logger.info("Starting insert_news_main script")

    # DB config
    db_config_path = os.path.join(
        project_root,
        "configs",
        "datasources",
        "local_test_db_config.yaml"
    )
    db_conf = load_db_config_yaml(db_config_path)

    # News config
    news_config_path = os.path.join(
        project_root, "configs", "datasources", "naver_news.yaml"
    )
    insert_news_main_core(news_config_path, db_conf)
    logger.info("insert_news_main script completed.")
