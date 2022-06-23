import pandas as pd
import os


# Step 1: Drop all rows that aren't transactions to the Ronin Gateway Contract AND drop all columns that aren't the date or amount

directory = '/data/covalent/slpc-log-events'  # Directory where I saved the logs from the "Data Mining" step

for filename in os.listdir(directory):  # Iterate over the log files in the directory
    f = os.path.join(directory, filename)
    if (os.path.isfile(f)):
        df = pd.read_csv(f, low_memory=False, usecols=['block_signed_at', 'raw_log_data', 'raw_log_topics_0', 'raw_log_topics_2'], compression='gzip')  # These are the columns that interest us
        df = df.loc[(df['raw_log_topics_0'] == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') & (df['raw_log_topics_2'] == '0x000000000000000000000000e35d62ebe18413d96ca2a2f7cf215bb21a406b4b')]  # First part of and statement is recognising transactions and second part is for checking that the recipient is the address of the Ronin Gateway Contract (which constitutes an withdrawl of SLP) 
        df = df[['block_signed_at', 'raw_log_data']]  # We keep only the columns that we need, namely date and amount
        df.to_csv('/data/daily-slp-withdrawn/preprocessed/' + filename, compression='gzip')  # Save to CSV


# Step 2: Convert date row to datetime class and convert SLP amounts from hex to decimal

directory = '/data/daily-slp-withdrawn/preprocessed'  # Directory where data from step 1 is saved

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        df = pd.read_csv(f, low_memory=False, compression='gzip')
        df = df.replace(['T', 'Z'], ' ', regex=True)  # Drop unnecessary information from date string 
        df['block_signed_at'] = pd.to_datetime(df['block_signed_at'])  # Convert string to datetime
        df['raw_log_data'] = df['raw_log_data'].apply(int, base=16)  # Convert SLP amounts from hex to decimal
        df.to_csv('/data/daily-slp-withdrawn/processed/' + filename, compression='gzip')

      
# Step 3: Concatenate everything into a single CSV

directory = '/data/daily-slp-withdrawn/processed'  # Directory where data from step 2 is saved
lst = []  # List where CSV data is accumulated (prior to saving)

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        df = pd.read_csv(f, low_memory=False, compression='gzip')
        lst.append(df)

final = pd.concat(lst)
final.to_csv('/data/daily-slp-withdrawn/slp-all-withdrawls.csv', compression='gzip')  # Export to CSV


# Step 4: Group emissions by date

df = pd.read_csv('/data/daily-slp-withdrawn/slp-all-withdrawls.csv', low_memory=False, usecols=['block_signed_at','raw_log_data'], compression='gzip')
df['block_signed_at'] = pd.to_datetime(df['block_signed_at'])
df = df.groupby([df['block_signed_at'].dt.date]).sum()  # Group by date

df.to_csv('/data/daily-slp-withdrawn/slp-all-withdrawls-grouped-by-date.csv')  # Export to CSV
