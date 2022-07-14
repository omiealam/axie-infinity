import pandas as pd
import os


# Step 1: Drop all rows that aren't transactions and keep only columns with time data and amount received

directory = '/data/covalent/slpc-log-events'  # Directory where I saved the logs from the "Data Mining" step

for filename in os.listdir(directory):  # Iterate over the log files in the directory
    f = os.path.join(directory, filename)
    if (os.path.isfile(f) and f.endswith('.gz')):
        df = pd.read_csv(f, low_memory=False, usecols=['raw_log_data', 'raw_log_topics_0', 'block_signed_at'], compression='gzip')  # These are the columns that interest us
        df = df.loc[(df['raw_log_topics_0'] == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')]  # This statement is for recognising transactions 
        df = df[['block_signed_at', 'raw_log_data']]  # Keep only amount received and recipient address
        df.to_csv('/data/amount-received/median/preprocessed/' + filename, compression='gzip')  # Save to CSV

        
# Step 2: Convert SLP amounts from hexidecimal to decimal while dropping 0 amounts

directory = '/data/amount-received/median/preprocessed/'  # Directory where data from step 1 is saved

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        df = pd.read_csv(f, low_memory=False, compression='gzip')
        df['raw_log_data'] = df['raw_log_data'].apply(int, base=16)  # Convert SLP amounts from hex to decimal
        df = df[df['raw_log_data'] != 0]  # Drop 0 amounts
        df.to_csv('/data/amount-received/median/processed/' + filename, compression='gzip')  # Export to CSV
        
      
# Step 3: Concatenate everything into a single CSV

directory = '/data/amount-received/median/processed/'  # Directory where data from step 2 is saved
lst = []  # List where CSV data is accumulated (prior to saving)

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        df = pd.read_csv(f, low_memory=False, compression='gzip')
        lst.append(df)

final = pd.concat(lst)
final.to_csv('/data/amount-received/median/all-received-amounts-by-date.csv.gz', compression='gzip')  # Export to CSV


# Step 4: Group amounts by wallet address

df = pd.read_csv('/data/amount-received/median/all-received-amounts-by-date.csv.gz', low_memory=False, usecols=['block_signed_at', 'raw_log_data'], compression='gzip')
df['block_signed_at'] = (pd.to_datetime(df['block_signed_at'])).dt.date
df = df.raw_log_data.groupby(df['raw_log_topics_2']).median()  # Group by date (median over amounts)

df.to_csv('/data/amount-received/median/median-slp-earned-by-date.csv')  # Export to CSV
