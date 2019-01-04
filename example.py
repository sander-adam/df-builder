import time
import pandas as pd
import random
from dataframeBuilder import DataFrameBuilder

# Let's see how long it takes
t = time.time()

# Make a new df_builder
df_builder = DataFrameBuilder()

# Create columns that consist of (higlevel, lowlevel) multi-index tuples (e.g. (LTC, pending) )
types = ['available', 'locked', 'pending']
currencies = ['BTC', 'ETH', 'LTC']
columns = [(c,t) for c in currencies for t in types]

# Build df with 100,000 
for _ in range(43200):
    now = time.time()
    df_builder.new_row(now)
    for c in columns:
        df_builder.add_value(c, random.random())

# Create a different set of columns to test robustness
currencies = ['BTC', 'ETH', 'XRP']
columns = [(c,t) for c in currencies for t in types]

for _ in range(43200):
    now = time.time()
    df_builder.new_row(now)
    for c in columns:
        df_builder.add_value(c, random.random())

# Make the dataframe
df = df_builder.to_df()

# Convert index to datetime, not that actual method required depends on the input format of the timestamps
df.index = pd.to_datetime(df.index, unit='s')

# Time it
e = time.time() - t
print(f'Elapsed {e*1000:,.0f}ms')

# Confirm correctness of df
print(df.dtypes) # Dtypes should be float for numeric data, not object
print(df.head(5))
print(df.tail(5))