import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. 환경 변수 로드 (.env에 적은 DB 정보를 가져옵니다)
load_dotenv()

db_user = os.getenv("DB_USER")
db_pw = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# DB 연결 엔진 생성
db_url = f"postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

# 2. Extract (수집): 구글 뉴스 RSS 피드(XML) 가져오기
print("📡 구글 뉴스 수집을 시작합니다...")
url = "https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko"
response = requests.get(url)
soup = BeautifulSoup(response.content, "xml") # XML 형식으로 읽기

# 뉴스 아이템 추출 (최신 10개)
items = soup.find_all("item")
news_list = []

for item in items[:10]:
    news_list.append({
        "title": item.title.text,
        "link": item.link.text,
        "pub_date": item.pubDate.text
    })

# 3. Transform (가공): 데이터를 표(DataFrame) 형태로 변환
df = pd.DataFrame(news_list)

# 4. Load (적재): 도커로 실행 중인 PostgreSQL에 저장
try:
    # 'daily_news'라는 테이블 이름으로 저장 (이미 있으면 덮어쓰기)
    df.to_sql('daily_news', engine, if_exists='replace', index=False)
    print(f"✅ 성공! {len(df)}개의 최신 뉴스가 DB에 적재되었습니다.")
except Exception as e:
    print(f"❌ 실패: {e}")