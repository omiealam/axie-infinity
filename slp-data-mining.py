from google.colab import drive
import time
import requests
import pandas as pd
import numpy as np
import urllib
import time
import io


drive.mount('drive')  # Mount Google Drive


#API keys
key = # Get your own key at covalenthq.com 


# The addresses of the 25 verified smart contracts that run Axie Infinity 
rwc     = '0xc99a6a985ed2cac1ef41640596c5a5f9f4e19ef5'
aisc    = '0x97a9107c1793bc407d6f527b77e7fff4d812bece'
slpc    = '0xa8754b9fa15fc18bb59458815510e40a12cd2014'
aecc    = '0x173a2d4fa585a63acd02c107d57f932be0a71bcc'
usdcc   = '0x0b7007c13325c48911f73a2dad5fa5dcbf808adc'
wrc     = '0xe514d9deb7966c8be0ca922de8a064264ea6bcd4'
ax      = '0x32950db2a7164ae833121501c797d79e7b79d74c' 
lc      = '0x8c811e3c958e190f5ec15fb376533a3398620500'
lic     = '0xa96660f0e4a3e9bc7388925d245a6d4d79e21259'
ec      = '0x2da06d60bd413bcbb6586430857433bd9d3a4be4'
mc      = '0x213073989821f738a7ba3520c3d31a1f9ad31bbd'
oac     = '0x5b16d12a0c2c88db94115968abd7afa78b6bc504'
kfc     = '0xb255d6a720bb7c39fee173ce22113397119cb930'
krc     = '0x7d0556d55ca1a92708681e2e231733ebd922597d'
awlpc   = '0xc6344bc1604fcab1a5aad712d766796e2b7a70b9'
swlpc   = '0x306a28279d04a47468ed83d55088d0dcd1369294'
uwlpc   = '0xa7964991f339668107e2b6a6f6b8e8b74aa9d017'
rwlpc   = '0x2ecb08f87f075b5769fe543d0e52e40140575ea7'
smc     = '0x8bd81a19420bad681b7bfc20e703ebd8e253782d'
aspc    = '0x05b0bb3c1c320b280501b86706c3551995bc8571'
awlpspc = '0x487671acdea3745b6dac3ae8d1757b44a04bfe8a'
swlpspc = '0xd4640c26c1a31cd632d8ae1a96fe5ac135d1eb52'
rwlpsc  = '0xb9072cec557528f81dd25dc474d4d69564956e1e'
rgc     = '0xe35d62ebe18413d96ca2a2f7cf215bb21a406b4b'
rvc     = '0x0000000000000000000000000000000000000011'


contracts = [slpc]      # The SLP contract interests us here   
start_block = 0         # Starting block
end_block = 12068802    # End block (as of 16:49 20.03.2022)
block_increment = 1000  # API requests are over a range of 1000 blocks 
page_size = 500000      # This is sufficient for the the number of log events in 1000 blocks (assumption of max 500 events per block on average)
url1 = 'https://api.covalenthq.com/v1/2020/events/address/'  # First part of API endpoint URL (here we select the type of query)
url2 = '/?quote-currency=USD&format=CSV'                     # Second part of API endpoint URL (select output format)


for contract in contracts:
  print(contract)    # See the name of the current contract (future-proofing for scenarios with multiple contracts)
  lst = []           # List where the log events are accumulated before being exported as CSV every 100000 blocks
  base_url = url1 + contract + url2  # Build base URL for API endpoint
  for i in range(start_block, end_block + 1, block_increment):  # Increment over total block range in increments of size block_increment      
    curr_url = base_url + '&starting-block=' + str(i) + '&ending-block=' + str(i + block_increment - 1) + '&page-size=' + str(page_size) + '&key=ckey_' + key  # Build rest of API URL providing API key, page size, starting and ending block 
    result = ""
    while (result == ""):  # Repeat GET requests until success
      try:
        result = requests.get(curr_url)  # Perform API request
        result = result.content
        rawData = pd.read_csv(io.StringIO(result.decode('utf-8')), on_bad_lines='skip')  # Read input as a CSV
        lst.append(rawData)  # Append to accumulator list
        if(i % 1000000 == 0 and i > start_block):  # Save to CSV every 100000 blocks
          print(i)
          df = pd.concat(lst)
          df.to_csv("drive/My Drive/" + contract + '/' + str(i) + '.csv')
          lst = []  # Empty accumulator list
        break
      except:
        time.sleep(5)  # If API request fails, sleep (gets around 429 issue - "Too Many Requests")
        continue
