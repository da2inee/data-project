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

    def analyze_trends(self, period='daily'):
        """
        DB에 저장된 데이터의 트렌드 분석
        period: 'daily', 'weekly', 'monthly'
        """
        if not self.engine:
            logger.error("DB 연결 실패")
            return
        
        logger.info(f"📈 {period.upper()} 트렌드 분석 시작")
        
        try:
            # 1. 날씨 트렌드
            df_weather = pd.read_sql(
                "SELECT * FROM weather_data ORDER BY collected_at",
                self.engine
            )
            
            # 2. 환율 트렌드
            df_exchange = pd.read_sql(
                "SELECT * FROM exchange_rate ORDER BY collected_at",
                self.engine
            )
            
            # 3. 공공 데이터 트렌드
            df_public = pd.read_sql(
                "SELECT * FROM public_data ORDER BY collected_at",
                self.engine
            )
            
            if df_weather.empty and df_exchange.empty and df_public.empty:
                logger.warning("⚠️ 분석할 데이터가 없습니다. 먼저 데이터를 수집해주세요.")
                return
            
            # 날짜 형식 변환
            for df in [df_weather, df_exchange, df_public]:
                if not df.empty and 'collected_at' in df.columns:
                    df['collected_at'] = pd.to_datetime(df['collected_at'])
                    df['date'] = df['collected_at'].dt.date
                    df['hour'] = df['collected_at'].dt.hour
            
            # 기간별 그룹화
            if period == 'daily':
                group_by = 'date'
                title_suffix = "일별"
            elif period == 'weekly':
                group_by = 'week'
                for df in [df_weather, df_exchange, df_public]:
                    if not df.empty:
                        df['week'] = df['collected_at'].dt.isocalendar().week
                title_suffix = "주별"
            else:  # monthly
                group_by = 'month'
                for df in [df_weather, df_exchange, df_public]:
                    if not df.empty:
                        df['month'] = df['collected_at'].dt.to_period('M')
                title_suffix = "월별"
            
            # 그래프 생성 (3x1 레이아웃)
            fig, axes = plt.subplots(3, 1, figsize=(12, 10))
            fig.suptitle(f'📊 {title_suffix} 데이터 트렌드 분석', fontsize=16, y=0.995)
            
            # 1. 날씨 트렌드
            if not df_weather.empty:
                weather_trend = df_weather.groupby(group_by).agg({
                    'temperature': 'mean',
                    'windspeed': 'mean'
                }).reset_index()
                
                ax1 = axes[0]
                ax1_twin = ax1.twinx()
                
                line1 = ax1.plot(weather_trend[group_by], weather_trend['temperature'], 
                                'o-', color='orangered', linewidth=2, label='Temperature (°C)')
                line2 = ax1_twin.plot(weather_trend[group_by], weather_trend['windspeed'], 
                                     's-', color='skyblue', linewidth=2, label='Wind Speed (km/h)')
                
                ax1.set_ylabel('Temperature (°C)', color='orangered')
                ax1_twin.set_ylabel('Wind Speed (km/h)', color='skyblue')
                ax1.set_title(f'🌡️ {title_suffix} 날씨 변화')
                ax1.grid(True, alpha=0.3)
                ax1.tick_params(axis='y', labelcolor='orangered')
                ax1_twin.tick_params(axis='y', labelcolor='skyblue')
                
                # 범례 통합
                lines = line1 + line2
                labels = [l.get_label() for l in lines]
                ax1.legend(lines, labels, loc='upper left')
            else:
                axes[0].text(0.5, 0.5, '날씨 데이터 없음', ha='center', va='center')
                axes[0].set_title('🌡️ 날씨 트렌드')
            
            # 2. 환율 트렌드
            if not df_exchange.empty:
                exchange_trend = df_exchange.groupby(group_by).agg({
                    'krw_rate': 'mean',
                    'eur_rate': 'mean',
                    'jpy_rate': 'mean'
                }).reset_index()
                
                ax2 = axes[1]
                ax2.plot(exchange_trend[group_by], exchange_trend['krw_rate'], 
                        'o-', label='USD/KRW', linewidth=2, color='green')
                
                ax2.set_ylabel('KRW per USD')
                ax2.set_title(f'💱 {title_suffix} 환율 변화 (USD/KRW)')
                ax2.grid(True, alpha=0.3)
                ax2.legend()
                
                # 최고/최저 표시
                max_idx = exchange_trend['krw_rate'].idxmax()
                min_idx = exchange_trend['krw_rate'].idxmin()
                ax2.plot(exchange_trend.loc[max_idx, group_by], 
                        exchange_trend.loc[max_idx, 'krw_rate'], 
                        'r^', markersize=10, label=f'최고: {exchange_trend.loc[max_idx, "krw_rate"]:.2f}')
                ax2.plot(exchange_trend.loc[min_idx, group_by], 
                        exchange_trend.loc[min_idx, 'krw_rate'], 
                        'bv', markersize=10, label=f'최저: {exchange_trend.loc[min_idx, "krw_rate"]:.2f}')
                ax2.legend()
            else:
                axes[1].text(0.5, 0.5, '환율 데이터 없음', ha='center', va='center')
                axes[1].set_title('💱 환율 트렌드')
            
            # 3. 공공 데이터 트렌드 (PM10)
            if not df_public.empty:
                public_trend = df_public.groupby(group_by).agg({
                    'pm10': 'mean',
                    'pm25': 'mean'
                }).reset_index()
                
                ax3 = axes[2]
                ax3.fill_between(range(len(public_trend)), 0, public_trend['pm10'], 
                                alpha=0.3, color='purple')
                ax3.plot(public_trend[group_by], public_trend['pm10'], 
                        'o-', label='PM10', linewidth=2, color='purple')
                ax3.plot(public_trend[group_by], public_trend['pm25'], 
                        's-', label='PM2.5', linewidth=2, color='orange')
                
                # 미세먼지 기준선
                ax3.axhline(y=80, color='red', linestyle='--', alpha=0.5, label='나쁨 기준 (80)')
                ax3.axhline(y=30, color='green', linestyle='--', alpha=0.5, label='좋음 기준 (30)')
                
                ax3.set_ylabel('미세먼지 농도 (μg/m³)')
                ax3.set_title(f'🏢 {title_suffix} 미세먼지 변화')
                ax3.grid(True, alpha=0.3)
                ax3.legend()
            else:
                axes[2].text(0.5, 0.5, '공공 데이터 없음', ha='center', va='center')
                axes[2].set_title('🏢 미세먼지 트렌드')
            
            # x축 레이블 회전
            for ax in axes:
                ax.tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            
            # 통계 출력
            logger.info(f"\n📊 {title_suffix} 통계 요약:")
            if not df_weather.empty:
                logger.info(f"  🌡️ 평균 온도: {df_weather['temperature'].mean():.1f}°C")
                logger.info(f"  🌡️ 최고/최저: {df_weather['temperature'].max():.1f}°C / {df_weather['temperature'].min():.1f}°C")
            if not df_exchange.empty:
                logger.info(f"  💱 평균 환율: {df_exchange['krw_rate'].mean():.2f} KRW")
                logger.info(f"  💱 최고/최저: {df_exchange['krw_rate'].max():.2f} / {df_exchange['krw_rate'].min():.2f}")
            if not df_public.empty:
                logger.info(f"  🏢 평균 PM10: {df_public['pm10'].mean():.1f}")
            
            logger.info("📊 트렌드 그래프를 화면에 띄웁니다...")
            plt.show()
            
        except Exception as e:
            logger.error(f"트렌드 분석 실패: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    pipeline = MultiSourcePipeline()
    
    # 옵션 1: 데이터 수집 + 실시간 시각화
    # pipeline.run_and_visualize()
    
    # 옵션 2: 데이터 수집만
    # pipeline.run_multi_pipeline()
    
    # 옵션 3: 트렌드 분석 (DB에 쌓인 데이터 분석)
    pipeline.analyze_trends(period='daily')  # 'daily', 'weekly', 'monthly'
