import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob

# 设置文件路径
file_pattern = 'VAR_*.csv'  # 假设所有文件都在当前目录下

# 获取所有符合条件的文件
file_list = glob.glob(file_pattern)

# 初始化一个空的DataFrame来存储结果
all_data = pd.DataFrame()

# 循环读取每个文件
for file in file_list:
    # 读取CSV文件
    data = pd.read_csv(file)

    # 计算日收益率变动
    data['daily_var_change'] = data.groupby('代码')['VaR'].diff()

    # 计算波动率
    data['var_volatility'] = data.groupby('代码')['daily_var_change'].rolling(window=20).std().reset_index(drop=True)

    # 将当前文件的数据添加到总数据中
    all_data = pd.concat([all_data, data], ignore_index=True)

# 绘制VaR波动率的时间序列图
plt.figure(figsize=(10, 6))
for code in all_data['代码'].unique():
    subset = all_data[all_data['代码'] == code]
    plt.plot(subset['date'], subset['var_volatility'], marker='o', label=f'Bond {code}')

plt.title('VaR Volatility Over Time')
plt.xlabel('Date')
plt.ylabel('Volatility')
plt.legend()
plt.grid(True)
plt.show()