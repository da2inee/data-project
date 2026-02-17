"""
고급 데이터 파이프라인 예시
- 스케줄링: 매일 자동 실행
- 에러 핸들링: 재시도 로직
- 로깅: 실행 기록 저장
- 데이터 검증: 품질 체크
"""

import os
import time
import logging
from datetime import datetime
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 환경 변수 로드
load_dotenv()

class DataPipeline:
    """ETL 파이프라인 클래스"""
    
    def __init__(self):
        """DB 연결 초기화"""
        self.engine = self._create_db_engine()
        self.max_retries = 3
        self.retry_delay = 5  # 초
        
    def _create_db_engine(self):
        """DB 엔진 생성"""
        try:
            db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            return create_engine(db_url)
        except Exception as e:
            logger.error(f"DB 연결 실패: {e}")
            return None
    
    def extract_news(self) -> List[Dict]:
        """
        Extract: 구글 뉴스 수집
        재시도 로직 포함
        """
        url = "https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko"
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"뉴스 수집 시도 {attempt + 1}/{self.max_retries}")
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, "xml")
                items = soup.find_all("item")
                
                news_list = []
                for item in items[:20]:  # 최신 20개
                    news_list.append({
                        "title": item.title.text,
                        "link": item.link.text,
                        "pub_date": item.pubDate.text,
                        "collected_at": datetime.now()
                    })
                
                logger.info(f"✅ {len(news_list)}개의 뉴스 수집 완료")
                return news_list
                
            except Exception as e:
                logger.warning(f"시도 {attempt + 1} 실패: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error("최대 재시도 횟수 초과")
                    return []
        
        return []
    
    def transform_data(self, news_list: List[Dict]) -> pd.DataFrame:
        """
        Transform: 데이터 정제 및 가공
        """
        if not news_list:
            logger.warning("변환할 데이터가 없습니다")
            return pd.DataFrame()
        
        try:
            df = pd.DataFrame(news_list)
            
            # 1. 중복 제거
            before_count = len(df)
            df = df.drop_duplicates(subset=['title'])
            logger.info(f"중복 제거: {before_count} → {len(df)}개")
            
            # 2. 제목 길이 추가
            df['title_length'] = df['title'].str.len()
            
            # 3. 날짜 형식 변환
            df['pub_date'] = pd.to_datetime(df['pub_date'])
            
            # 4. 데이터 검증
            self._validate_data(df)
            
            logger.info("✅ 데이터 변환 완료")
            return df
            
        except Exception as e:
            logger.error(f"데이터 변환 실패: {e}")
            return pd.DataFrame()
    
    def _validate_data(self, df: pd.DataFrame):
        """데이터 품질 검증"""
        # 필수 컬럼 확인
        required_columns = ['title', 'link', 'pub_date']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"필수 컬럼 누락: {missing}")
        
        # 빈 값 확인
        null_counts = df[required_columns].isnull().sum()
        if null_counts.any():
            logger.warning(f"빈 값 발견:\n{null_counts[null_counts > 0]}")
        
        logger.info("✅ 데이터 검증 통과")
    
    def load_to_db(self, df: pd.DataFrame, table_name: str = 'daily_news'):
        """
        Load: 데이터베이스에 적재
        """
        if df.empty:
            logger.warning("적재할 데이터가 없습니다")
            return False
        
        if not self.engine:
            logger.error("DB 엔진이 초기화되지 않았습니다")
            return False
        
        try:
            # append 모드: 기존 데이터에 추가
            df.to_sql(table_name, self.engine, if_exists='append', index=False)
            logger.info(f"✅ {len(df)}개의 레코드를 '{table_name}' 테이블에 적재 완료")
            return True
            
        except Exception as e:
            logger.error(f"DB 적재 실패: {e}")
            return False
    
    def run_pipeline(self):
        """전체 파이프라인 실행"""
        logger.info("=" * 50)
        logger.info("🚀 ETL 파이프라인 시작")
        logger.info("=" * 50)
        
        start_time = time.time()
        
        # Extract
        news_list = self.extract_news()
        
        # Transform
        df = self.transform_data(news_list)
        
        # Load
        success = self.load_to_db(df)
        
        # 실행 시간
        elapsed_time = time.time() - start_time
        logger.info(f"⏱️  실행 시간: {elapsed_time:.2f}초")
        
        if success:
            logger.info("✅ 파이프라인 실행 성공")
        else:
            logger.error("❌ 파이프라인 실행 실패")
        
        logger.info("=" * 50)
        return success


def schedule_pipeline():
    """
    스케줄러 설정
    매일 오전 9시와 오후 6시에 자동 실행
    """
    scheduler = BlockingScheduler()
    pipeline = DataPipeline()
    
    # 매일 09:00에 실행
    scheduler.add_job(pipeline.run_pipeline, 'cron', hour=9, minute=0)
    
    # 매일 18:00에 실행
    scheduler.add_job(pipeline.run_pipeline, 'cron', hour=18, minute=0)
    
    logger.info("📅 스케줄러 시작 (매일 09:00, 18:00 실행)")
    logger.info("중지하려면 Ctrl+C를 누르세요")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("스케줄러 종료")


if __name__ == "__main__":
    # 방법 1: 즉시 실행
    pipeline = DataPipeline()
    pipeline.run_pipeline()
    
    # 방법 2: 스케줄링 (주석 해제하면 자동 실행)
    # schedule_pipeline()
