import pandas as pd

df = pd.read_csv(r'C:\Users\du\Desktop\PyDeveloper\FixlaneWorkBrief\test data\PAS\V201_NORTHLAND_0.2m-Reduced_test_complete_chunked_20251010.csv', nrows=5)
print('Output columns:')
for i, col in enumerate(df.columns):
    print(f'{i+1:3d}: {col}')
print(f'\nTotal columns: {len(df.columns)}')
print(f'Has Ignore: {"Ignore" in df.columns}')
print(f'Has InBrief: {"InBrief" in df.columns}')