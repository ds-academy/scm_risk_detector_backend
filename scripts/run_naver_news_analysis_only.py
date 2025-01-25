import os
import sys
import logging

from module.utils import read_config, create_pipelines
from module.logger import get_logger, setup_global_logging
from module.data.providers.news_pipeline import NewsDataPipeline

logger = get_logger(__name__)


def run_naver_news_analysis_only(config_path: str):
    """
    이미 news_link.csv, contents.json 이 준비되어 있다고 가정하고
    GPT 감성 분석만 수행하는 함수.
    """
    # 1) config 로드
    config = read_config(config_path)

    # 2) pipeline 생성
    pipelines = create_pipelines(config)

    # 3) NewsDataPipeline 변환
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

    # (C) 감성 분석만 수행
    openai_key = os.getenv("OPENAI_API_KEY", "")
    if not openai_key:
        logger.warning("OPENAI_API_KEY not found. Skipping sentiment analysis.")
    else:
        for pipeline in news_pipelines:
            logger.info(f"Starting GPT analysis for symbol: {pipeline.data_provider.symbol}")
            pipeline.analyze_contents_with_gpt(api_key=openai_key, model_id="gpt-4o-mini")
    logger.info("Naver news analysis completed.")


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

    logger.info("Starting Naver News analysis-only script")

    # 기존 config_path
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
        run_naver_news_analysis_only(config_path)
        logger.info("Analysis-only script completed")
    except Exception as e:
        logger.error(f"Exception occurred: {e}", exc_info=True)
        sys.exit(1)
