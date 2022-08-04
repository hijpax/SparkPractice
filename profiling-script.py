import pandas as pd
import pandas_profiling

print("Read data ...")
df = pd.read_csv("../data/2019-Dec.csv")
df.head()

df.profile_report()
