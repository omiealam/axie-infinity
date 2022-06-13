import pandas as pd
import os


# Step 1: Drop all rows that aren't successful auctions and keep only the date and Axie ID column

directory = '/data/covalent/mkt-log-events'  # Directory where I saved the Marketplace smart contract logs from the "Data Mining" step

for filename in os.listdir(directory):  # Iterate over the log files in the directory
    f = os.path.join(directory, filename)
    if (os.path.isfile(f) and f.endswith('.gz')):
        df = pd.read_csv(f, compression='gzip', error_bad_lines=False, usecols=['decoded_name', 'block_signed_at', 'decoded_params_name', 'decoded_params_value'])  # Only read in the relevant columns
        df = df.loc[(df['decoded_name'] == 'AuctionSuccessful') & (df['decoded_params_name'] == '_token')]  # Only keeps rows which correspond to the token IDs of succesful auctions
        df = df[['block_signed_at', 'decoded_params_value']]  # Only keep time and token ID values in final CSV
        df.to_csv('/data/mkt-data/daily-axies-sold/preprocessed/' + filename, compression='gzip') # Export to CSV

        
# Step 2: Convert date (in string) format to python datetime module

directory = '/data/mkt-data/daily-axies-sold/preprocessed/'  # Directory where data from step 1 is saved

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        df = pd.read_csv(f, low_memory=False, compression='gzip')
        df = df.replace(['T', 'Z'], ' ', regex=True)  # Drop unnecessary characters from date string
        df['block_signed_at'] = pd.to_datetime(df['block_signed_at']) # Convert to python datetime
        df.to_csv('/data/mkt-data/daily-axies-sold/processed/' + filename, compression='gzip')  # Export to CSV


# Step 3: # Step 3: Concatenate everything into a single CSV

directory = '/data/mkt-data/daily-axies-sold/processed/'  # Directory where data from step 2 is saved
lst = []  # List where CSV data is accumulated (prior to final saving)

for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if ((os.path.isfile(f))):
        df = pd.read_csv(f, low_memory=False, compression='gzip')
        lst.append(df)

final = pd.concat(lst)
final.to_csv('/data/mkt-data/daily-axies-sold/all-axies-sold-with-datetime.csv.gz', compression='gzip')


# Step 4: Group Axies sold by date and sum amount (by day)

df = pd.read_csv('/data/mkt-data/daily-axies-sold/all-axies-sold-with-datetime.csv.gz', compression='gzip', low_memory=False, usecols=['block_signed_at','decoded_params_value'])
df['block_signed_at'] = pd.to_datetime(df['block_signed_at']) 
df = df.groupby([df['block_signed_at'].dt.date]).sum() # Group by date (summing amounts)

df.to_csv('/data/mkt-data/daily-axies-sold/daily-axies-sold.csv') # Export to CSV
