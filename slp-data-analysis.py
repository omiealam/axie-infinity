import pandas as pd
import os


# Step 1: Drop all rows that aren't transactions from the null address AND drop all columns that aren't the date or amount

directory = '/data/covalent/slpc-log-events'  # Directory where I saved the logs from the "Data Mining" step

for filename in os.listdir(directory):  # Iterate over the log files in the directory
    f = os.path.join(directory, filename)
    if (os.path.isfile(f)):
        df = pd.read_csv(f, low_memory=False, usecols=['block_signed_at', 'raw_log_data', 'raw_log_topics_0', 'raw_log_topics_1'])  # These are the columns that interest us
        df = df.loc[(df['raw_log_topics_0'] == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') & (df['raw_log_topics_1'] == '0x0000000000000000000000000000000000000000000000000000000000000000')]  # First part of and statement is recognising transactions and second part is for checking that the sender is the null address (which constitutes an emission of currency) 
        df = df[['block_signed_at', 'raw_log_data']]  # We keep only the columns that we need, namely date 
        df.to_csv('/data/slpc-emissions/' + filename)  # Save to CSV


# Step 2: Convert date row to datetime class and convert SLP amounts from hex to decimal

directory = '/data/slpc-emissions'  # Directory where data from step 1 is saved

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        df = pd.read_csv(f, low_memory=False)
        df = df.replace(['T', 'Z'], ' ', regex=True)  # Drop unnecessary information from date string 
        df['block_signed_at'] = pd.to_datetime(df['block_signed_at'])  # Convert string to datetime
        df['raw_log_data'] = df['raw_log_data'].apply(int, base=16)  # Convert SLP amounts from hex to decimal
        df.to_csv('/data/slpc-emissions-processed/' + filename)

      
# Step 3: Concatenate everything into a single CSV

directory = '/data/slpc-emissions-processed'  # Directory where data from step 2 is saved
lst = []  # List where CSV data is accumulated (prior to saving)

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        print(filename)
        df = pd.read_csv(f, low_memory=False)
        lst.append(df)

final = pd.concat(lst)
final.to_csv('/data/slpc-all-emissions.csv')  # Export to CSV


# Step 4: Group emissions by date

df = pd.read_csv('/data/slpc-all-emissions.csv', low_memory=False, usecols=['block_signed_at','raw_log_data'])
df['block_signed_at'] = pd.to_datetime(df['block_signed_at'])
df = df.groupby([df['block_signed_at'].dt.date]).sum()  # Group by date

df.to_csv('/data/slpc-all-emissions-grouped.csv')  # Export to CSV


