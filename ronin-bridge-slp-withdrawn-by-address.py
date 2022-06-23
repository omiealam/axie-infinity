import pandas as pd
import os


# Step 1: Drop all rows that aren't transactions to the Ronin Gateway Contract AND drop all columns that aren't the date or amount

directory = '/data/covalent/slpc-log-events'  # Directory where I saved the logs from the "Data Mining" step

for filename in os.listdir(directory):  # Iterate over the log files in the directory
    f = os.path.join(directory, filename)
    if (os.path.isfile(f)):
        df = pd.read_csv(f, low_memory=False, usecols=['raw_log_data', 'raw_log_topics_0', 'raw_log_topics_1', 'raw_log_topics_2'], compression='gzip')  # These are the columns that interest us
        df = df.loc[(df['raw_log_topics_0'] == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') & (df['raw_log_topics_2'] == '0x000000000000000000000000e35d62ebe18413d96ca2a2f7cf215bb21a406b4b')]  # First part of and statement is recognising transactions and second part is for checking that the recipient is the address of the Ronin Gateway Contract (which constitutes an withdrawl of SLP) 
        df = df[['raw_log_data', 'raw_log_topics_1']]  # We keep only the columns that we need, namely date and sender address
        df.to_csv('/data/slp-withdrawn-by-address/preprocessed/' + filename, compression='gzip')  # Save to CSV


# Step 2: Convert SLP amounts from hex to decimal

directory = '/data/slp-withdrawn-by-address/preprocessed/'  # Directory where data from step 1 is saved

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        df = pd.read_csv(f, low_memory=False, compression='gzip')
        df['raw_log_data'] = df['raw_log_data'].apply(int, base=16)  # Convert SLP amounts from hex to decimal
        df.to_csv('/data/slp-withdrawn-by-address/processed/' + filename, compression='gzip')

      
# Step 3: Concatenate everything into a single CSV

directory = '/data/slp-withdrawn-by-address/processed/'  # Directory where data from step 2 is saved
lst = []  # List where CSV data is accumulated (prior to saving)

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        df = pd.read_csv(f, low_memory=False, compression='gzip')
        lst.append(df)

final = pd.concat(lst)
final.to_csv('/data/slp-withdrawn-by-address/slp-withdrawn-by-non-unique-address', compression='gzip')  # Export to CSV


# Step 4: Group emissions by date

df = pd.read_csv('/data/slp-withdrawn-by-address/slp-withdrawn-by-non-unique-address', low_memory=False, usecols=['raw_log_data', 'raw_log_topics_1'], compression='gzip')
df = df.raw_log_data.groupby(df['raw_log_topics_2']).sum()  # Group by address (summing amounts)

df.to_csv('/data/slp-withdrawn-by-address/slp-withdrawn-by-unique-address')  # Export to CSV
