from matplotlib import pyplot as plt
import pandas as pd

# CSV 불러오기
df = pd.read_csv("netflix_titles.csv")

# 데이터 확인
print("데이터 형태:", df.shape)
print(df.head())
print(df.info())
print(df.describe(include="all"))
print(df.isnull().sum())

# 문자열 데이터 결측치 'Unknown'으로 채우기
df['country'] = df['country'].fillna("Unknown")
df['cast'] = df['cast'].fillna("Unknown")
df['director'] = df['director'].fillna("Unknown")

# 숫자 데이터 결측치는 평균/중앙값으로 채우기
df['release_year'] = df['release_year'].fillna(df['release_year'].median())

# figure와 subplot 생성: 1행 2열
fig, axes = plt.subplots(1, 2, figsize=(12, 5))

# 1️⃣ 제거 전
axes[0].boxplot(df['release_year'])
axes[0].set_title('Before Removing Outliers')

# 2️⃣ 제거 후
# 이상치 제거
df_filtered = df[(df['release_year'] >= 1950) & (df['release_year'] <= 2025)]
axes[1].boxplot(df_filtered['release_year'])
axes[1].set_title('After Removing Outliers')

plt.tight_layout()
plt.show()

# 예: 날짜 컬럼에서 연도/월 분리
df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
df['date_added'].fillna(method='ffill', inplace=True)  # 앞값으로 채우기
df['year_added'] = df['date_added'].dt.year
df['month_added'] = df['date_added'].dt.month

# 예: 문자열 데이터에서 다중 값 분리
df['num_cast'] = df['cast'].apply(lambda x: len(str(x).split(',')))

# 국가별 콘텐츠 개수 (혹은 고객 수)
df['country'].value_counts().head(10).plot(kind='bar')
plt.title("Top 10 Countries")
plt.show()

# 연도별 콘텐츠/구매 트렌드
df['year_added'].value_counts().sort_index().plot(kind='line')
plt.title("Content Added by Year")
plt.show()
