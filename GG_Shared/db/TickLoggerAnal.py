import warnings
warnings.filterwarnings("ignore")
import datetime
import itertools
import math
import os.path
import pickle
import time
import empyrical as emp
import matplotlib
import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import mysql.connector
import numpy as np
import pandas as pd
import pandas.io.sql as pdsql
import seaborn as sns
import talib as ta
from matplotlib import dates
from pandas import DataFrame
from pandasql import sqldf
from scipy.stats import norm

sns.set(style="whitegrid", font="Malgun Gothic", font_scale=1.0)
matplotlib.rcParams["figure.figsize"] = [12, 8]
fp = font_manager.FontProperties(fname="C:\\WINDOWS\\Fonts\\malgun.TTF", size=10)

def comma_volume(x, pos): return "{:0,d}K".format(int(x / 1000))
def comma_price(x, pos): return "{:0,d}".format(int(x))
def comma_percent(x, pos): return "{:+.2f}".format(x)

major_date_formatter = dates.DateFormatter("%Y-%m-%d")
minor_date_formatter = dates.DateFormatter("%m")
price_formatter = ticker.FuncFormatter(comma_price)
volume_formatter = ticker.FuncFormatter(comma_volume)
percent_formatter = ticker.FuncFormatter(comma_percent)

columns = ["체결시간","전일대비구분","전일대비","등락율","현재가","시가시간","시가","고가시간","고가","저가시간","저가","체결구분","체결량","거래량","누적거래대금","매도누적체결량","매도누적체결건수","매수누적체결량","매수누적체결건수","체결강도","가중평균가","매도호가","매수호가","장정보","전일동시간대거래량","종목코드"]
df = pd.read_csv("006490.csv", names=columns, encoding="cp949", header=0)
df["datetime"] = df["체결시간"].apply(lambda x: datetime.datetime.strptime("2018-04-25 %s" % x, "%Y-%m-%d %H%M%S"))
df.set_index("datetime", inplace=True)
df["SIGN"] = df["체결구분"].apply(lambda x: 1 if x == "+" else -1)
df["SUMSIGN"] = df["SIGN"].cumsum()
df.head()

figsize = (25, 10)
fig, axes = plt.subplots(1, 1, figsize=figsize, sharex=True)
ax = axes
ax = df[["현재가"]].plot(ax=ax)
ax.legend(loc="best")
ax = axes.twinx()
ax = df[["매도누적체결량", "매수누적체결량"]].plot(ax=ax)
ax.legend(loc="best")
plt.show()
