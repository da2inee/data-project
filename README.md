# data-project
dataset train projects

# 🚀 Data & AI Journey (AX 프로젝트)

데이터 엔지니어링 기초부터 AI 전환(AX)까지의 학습 과정을 기록하는 저장소입니다.

## 🛠 Tech Stack
- **Language:** Python 3.9+
- **Database:** PostgreSQL (Docker)
- **Libraries:** Pandas, SQLAlchemy, BeautifulSoup4, python-dotenv

---

## 🧪 Experiments & Insights

### [실험 01] 도커 기반 격리된 DB 환경 구축
- **내용:** 로컬 PC에 직접 설치하지 않고 Docker 컨테이너를 이용해 PostgreSQL 환경을 구축함.
- **핵심:** `-p 5432:5432` 포트 포워딩과 환경변수(`-e`) 설정을 통해 인프라 구성 능력을 익힘.
- **인사이트:** 개발 환경의 일관성을 유지하기 위해 도커가 왜 필수적인지 체감함.

### [실험 02] 실시간 뉴스 수집(ETL) 파이프라인 완성
- **내용:** 구글 뉴스 RSS 피드를 크롤링하여 PostgreSQL에 자동 적재하는 파이썬 스크립트 작성.
- **핵심:** - `python-dotenv`를 활용해 DB 접속 정보를 안전하게 분리함.
  - `BeautifulSoup`으로 비정형 XML 데이터를 데이터프레임으로 변환.
  - `to_sql` 기능을 활용해 DB 적재 자동화.
- **인사이트:** 데이터 수집(Extract) → 가공(Transform) → 적재(Load)라는 데이터 엔지니어링의 핵심 사이클을 이해함.