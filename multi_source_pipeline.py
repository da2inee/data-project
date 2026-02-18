"""
다중 소스 데이터 파이프라인
- 여러 API에서 데이터 수집
- 데이터 통합 및 분석
"""

import os
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
import requests
import matplotlib.pyplot as plt

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()


class MultiSourcePipeline:
    """여러 소스에서 데이터를 수집하는 파이프라인"""
    
    def __init__(self):
        self.engine = self._create_db_engine()
    
    def _create_db_engine(self):
        """DB 연결"""
        try:
            db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            return create_engine(db_url)
        except:
            return None
    
    # 소스 1: 무료 날씨 API
    def extract_weather(self, city: str = "Seoul") -> dict:
        """날씨 데이터 수집 (예시: Open-Meteo)"""
        try:
            # 서울 좌표
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": 37.5665,
                "longitude": 126.9780,
                "current_weather": True
            }
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            weather = data['current_weather']
            return {
                'city': city,
                'temperature': weather['temperature'],
                'windspeed': weather['windspeed'],
                'weathercode': weather['weathercode'],
                'collected_at': datetime.now()
            }
        except Exception as e:
            logger.error(f"날씨 데이터 수집 실패: {e}")
            return {}
    
    # 소스 2: 환율 정보 (Mock 예시)
    def extract_exchange_rate(self) -> dict:
        """환율 정보 수집"""
        try:
            # 무료 환율 API (예시)
            url = "https://api.exchangerate-api.com/v4/latest/USD"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            return {
                'base_currency': 'USD',
                'krw_rate': data['rates'].get('KRW', 0),
                'eur_rate': data['rates'].get('EUR', 0),
                'jpy_rate': data['rates'].get('JPY', 0),
                'collected_at': datetime.now()
            }
        except Exception as e:
            logger.error(f"환율 데이터 수집 실패: {e}")
            return {}
    
    # 소스 3: 공공 데이터 (예시)
    def extract_public_data(self) -> dict:
        """공공 API 데이터 수집 예시"""
        # 실제로는 공공데이터포털 API 키가 필요
        # 여기서는 Mock 데이터
        return {
            'data_type': 'air_quality',
            'pm10': 45,
            'pm25': 25,
            'status': 'good',
            'collected_at': datetime.now()
        }
    
    def run_multi_pipeline(self):
        """모든 소스에서 데이터 수집"""
        logger.info("🌐 다중 소스 데이터 파이프라인 시작")
        
        # 1. 날씨 데이터
        logger.info("☁️  날씨 데이터 수집 중...")
        weather = self.extract_weather()
        if weather:
            df_weather = pd.DataFrame([weather])
            if self.engine:
                df_weather.to_sql('weather_data', self.engine, if_exists='append', index=False)
                logger.info(f"✅ 날씨 데이터 저장: {weather['temperature']}°C")
        
        # 2. 환율 데이터
        logger.info("💱 환율 데이터 수집 중...")
        exchange = self.extract_exchange_rate()
        if exchange:
            df_exchange = pd.DataFrame([exchange])
            if self.engine:
                df_exchange.to_sql('exchange_rate', self.engine, if_exists='append', index=False)
                logger.info(f"✅ 환율 데이터 저장: 1 USD = {exchange['krw_rate']} KRW")
        
        # 3. 공공 데이터
        logger.info("🏢 공공 데이터 수집 중...")
        public = self.extract_public_data()
        if public:
            df_public = pd.DataFrame([public])
            if self.engine:
                df_public.to_sql('public_data', self.engine, if_exists='append', index=False)
                logger.info(f"✅ 공공 데이터 저장: PM10={public['pm10']}")
        
        logger.info("✅ 모든 데이터 수집 완료")
        
        # 4. 데이터 통합 분석 (옵션)
        self.analyze_collected_data()
    
    def analyze_collected_data(self):
        """수집된 데이터 간단 분석"""
        if not self.engine:
            return
        
        try:
            logger.info("\n📊 데이터 분석 요약:")
            
            # 테이블별 레코드 수 확인
            tables = ['weather_data', 'exchange_rate', 'public_data']
            for table in tables:
                try:
                    df = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", self.engine)
                    logger.info(f"  - {table}: {df['count'].iloc[0]}개 레코드")
                except:
                    logger.info(f"  - {table}: 테이블 없음")
        
        except Exception as e:
            logger.error(f"분석 실패: {e}")

    def run_and_visualize(self):
        """데이터 수집 후 즉시 시각화 분석"""
        logger.info("🚀 데이터 수집 및 분석 시작")
        
        # 데이터 수집
        weather = self.extract_weather()
        exchange = self.extract_exchange_rate()
        public = self.extract_public_data()

        # 시각화를 위한 데이터프레임 생성
        # (실제 운영 시에는 DB에서 과거 데이터를 불러와서 그립니다)
        data = {
            'Category': ['Temp (°C)', 'USD/KRW (1/100)', 'PM10'],
            'Value': [
                weather.get('temperature', 0),
                exchange.get('krw_rate', 0) / 100, # 수치 맞춤을 위해 100으로 나눔
                public.get('pm10', 0)
            ]
        }
        df_plot = pd.DataFrame(data)

        # --- Matplotlib 시각화 영역 ---
        plt.figure(figsize=(10, 6))
        
        # 막대 그래프 그리기
        bars = plt.bar(df_plot['Category'], df_plot['Value'], color=['orange', 'skyblue', 'green'])
        
        # 수치 표시
        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + 0.5, yval, ha='center', va='bottom')

        plt.title("Real-time Data Snapshot")
        plt.ylabel("Value")
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        logger.info("📊 그래프를 화면에 띄웁니다...")
        plt.show() # 이 코드가 실행되면 팝업 창이 뜹니다.


if __name__ == "__main__":
    pipeline = MultiSourcePipeline()
    pipeline.run_and_visualize()  # 시각화 포함 버전
