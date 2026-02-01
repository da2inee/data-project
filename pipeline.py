import pandas as pd
from sqlalchemy import create_engine

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 1. 수집 (Extract): 연습용으로 간단한 리스트 데이터를 만듭니다. (나중엔 API나 CSV가 되겠죠?)
data = [
    {"name": "AI 기초", "category": "AX", "level": 1},
    {"name": "데이터 엔지니어링", "category": "Data", "level": 2},
    {"name": "도커 마스터", "category": "Infra", "level": 3}
]
df = pd.DataFrame(data)

# 2. 가공 (Transform): 레벨이 2 이상인 데이터만 필터링해봅니다.
filtered_df = df[df['level'] >= 2]

# 3. 적재 (Load): 도커로 띄운 PostgreSQL에 저장합니다.
# 형식: postgresql://아이디:비밀번호@호스트:포트/데이터베이스이름

# .env 파일 로드
load_dotenv()

# 환경 변수에서 정보 가져오기
db_user = os.getenv("DB_USER")
db_pw = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# f-string으로 주소 생성
engine = create_engine(f'postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}')

try:
    filtered_df.to_sql('study_list', engine, if_exists='replace', index=False)
    print("✅ 데이터 파이프라인 실행 성공! DB에 데이터가 저장되었습니다.")
except Exception as e:
    print(f"❌ 에러 발생: {e}")