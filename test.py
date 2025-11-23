import pandas as pd
import matplotlib.pyplot as plt

# 테스트용 데이터프레임
data = {
    'age': [20, 30, 40, 50],
    'salary': [2000, 3000, 4000, 5000]
}

df = pd.DataFrame(data)
print(df)

df['salary'].plot(kind='line')
plt.show()
