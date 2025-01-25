import os
import sys
import logging

from module.utils import read_config, create_pipelines
from module.logger import get_logger, setup_global_logging
from module.data.providers.news_pipeline import NewsDataPipeline

logger = get_logger(__name__)


def run_naver_news_pipeline(config_path: str):
    # 1) config 읽기
    config = read_config(config_path)

    # 2) pipeline 생성
    pipelines = create_pipelines(config)

    news_pipelines = []
    for p in pipelines:
        news_pipeline = NewsDataPipeline(
            data_provider=p.data_provider,
            base_path=p.base_path,
            use_file_lock=p.use_file_lock,
            cache_days=p.cache_days,
            fetch_interval=p.fetch_interval,
            chunk_size=p.chunk_size
        )
        news_pipelines.append(news_pipeline)

    # (A) 메타 수집
    for pipeline in news_pipelines:
        df_new = pipeline.fetch_data()
        logger.info(f"Fetched {len(df_new)} news items for query='{pipeline.data_provider.symbol}'")

    # (B) 본문 수집
    for pipeline in news_pipelines:
        pipeline.fetch_article_content()

    # (C) 감성 분석
    openai_key = os.getenv("OPENAI_API_KEY", "")
    if not openai_key:
        logger.warning("OPENAI_API_KEY not found. Skipping sentiment analysis.")
    else:
        for pipeline in news_pipelines:
            pipeline.analyze_contents_with_gpt(api_key=openai_key)
    logger.info("Naver news pipeline completed.")


if __name__ == "__main__":
    # 현재 파일의 위치와 프로젝트 루트 경로 설정
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    sys.path.append(project_root)

    # 전역 로깅 설정
    setup_global_logging(
        log_dir=os.path.join(project_root, "logs"),
        log_level=logging.INFO,
        file_level=logging.DEBUG,
        stream_level=logging.INFO,
        # telegram_token과 telegram_chat_id는 필요한 경우 추가
    )

    logger.info("Starting Naver News script")

    # 명령줄 인자 대신, 아래와 같이 config_path를 고정
    # 예: naver_news_config.yaml 파일 위치가 "configs/datasources/naver_news_config.yaml"라고 가정
    config_path = os.path.join(
        project_root,
        "configs",
        "datasources",
        "naver_news.yaml"
    )

    if not os.path.exists(config_path):
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)

    try:
        run_naver_news_pipeline(config_path)
        logger.info("Script completed")
    except Exception as e:
        logger.error(f"Exception occurred: {e}", exc_info=True)
        sys.exit(1)
