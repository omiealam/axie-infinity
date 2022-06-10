import pandas as pd
import os


# Step 1: Drop all rows that aren't transactions from the null address (claims) and keep only recipients and amount received

directory = '/data/covalent/slpc-log-events'  # Directory where I saved the logs from the "Data Mining" step

for filename in os.listdir(directory):  # Iterate over the log files in the directory
    f = os.path.join(directory, filename)
    if (os.path.isfile(f) and f.endswith('.gz')):
        df = pd.read_csv(f, low_memory=False, usecols=['block_signed_at', 'raw_log_data', 'raw_log_topics_0', 'raw_log_topics_1', 'raw_log_topics_2'], compression='gzip')  # These are the columns that interest us
        df = df.loc[(df['raw_log_topics_0'] == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') & (df['raw_log_topics_1'] == '0x0000000000000000000000000000000000000000000000000000000000000000')]  # First part of and statement is recognising transactions and second part is for checking that the sender is the null address (which counts as a claim) 
        df = df[['raw_log_topics_2', 'raw_log_data']]  # Keep only time/date and receiver address
        df.to_csv('/data/amount-received/slp-claims/preprocessed/' + filename, compression='gzip')  # Save to CSV

        
# Step 2: Convert SLP amounts from hexidecimal to decimal

directory = '/data/amount-received/slp-claims/preprocessed/'  # Directory where data from step 1 is saved

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        df = pd.read_csv(f, low_memory=False, compression='gzip')
        df['raw_log_data'] = df['raw_log_data'].apply(int, base=16)  # Convert SLP amounts from hex to decimal
        df.to_csv('/data/amount-received/slp-claims/processed/' + filename, compression='gzip')  # Export to CSV
        
      
# Step 3: Concatenate everything into a single CSV

directory = '/data/amount-received/slp-claims/processed/'  # Directory where data from step 2 is saved
lst = []  # List where CSV data is accumulated (prior to saving)

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        df = pd.read_csv(f, low_memory=False, compression='gzip')
        lst.append(df)

final = pd.concat(lst)
final.to_csv('/data/amount-received/slp-claims/total-claims-non-unique-address.csv.gz', compression='gzip')  # Export to CSV


# Step 4: Group total claims by address

df = pd.read_csv('/data/omar-thesis/amount-received/slp-claims/total-claims-unique-address.csv.gz', low_memory=False, usecols=['raw_log_topics_2', 'raw_log_data'])
df = df.raw_log_topics_2.groupby(df['raw_log_data']).nunique()  # Group by address (summing amounts)

df.to_csv('/data/amount-received/slp-claims/total-claims-unique-address.csv')  # Export to CSV
