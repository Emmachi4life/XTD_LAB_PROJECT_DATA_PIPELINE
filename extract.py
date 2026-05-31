import asyncio # this is the library for asynchronous programming in python
import aiohttp # this is the library for making asychronous http request
import os
import json # this is working with json data
from datetime import date, timedelta # this is library for working with date and time

# Define bronze path layer
BRONZE_PATH = "data/bronzes"
os.makedirs(BRONZE_PATH, exist_ok = True) #creating directory if it does not exist

#rate limiting with semaphores
semaphore = asyncio.Semaphore(5)

#coroutine: for the request operation logic
async def fectch_carbon_data(session,target_date):
    url = f"https://api.carbonintensity.org.uk/regional/intensity/{target_date}/pt24h"
    #file path to store raw data
    file_path = os.path.join(BRONZE_PATH, f"{target_date}.json")
    if os.path.exists(file_path):
        return #skip ifn the target data is available in the bronze layer
    async with semaphore:
        for attempts in range(3): # try making 3 request, if it fails, 
            try:
                async with session.get(url, timeout=20) as response:
                    if response.status == 200:
                        data = await response.json()
                        with open(file_path, "w") as f:
                            json.dump(data['data'], f)
                        print(f"Saved: {target_date}")
                    elif response.status == 429: # too many requests
                        print(f"Rate Limited on {target_date}. Waiting...")
                        await asyncio.sleep(5 * (attempts + 1))
                    else:
                        print(f"FAILED STATUS FOR {target_date}")
            except Exception as e:
                print(f"Retry {attempts + 1} for {target_date} due to {e}")
                await asyncio.sleep(2)
        print(f"FAILED after 3 attempts: {target_date}")

# Event Loop: Responsible for duplication and control of the coroutines asychronously
async def run_extraction():
    start_date = date(2022, 1, 1)
    end_date = date(2024, 12, 31)
    current = start_date # pointer to iterate through the date range
    
    tasks = []
    # Apply rate limiting to reduce simultaneous connections (this ultimately prevents IP bans)
    connector = aiohttp.TCPConnector(limit=5) 
    async with aiohttp.ClientSession(connector=connector) as session:
        while current <= end_date:
            tasks.append(fectch_carbon_data(session, current))
            current += timedelta(days=1)
        # gather all deuplicated tasks and run them concurrently
        await asyncio.gather(*tasks)

# To run: 
asyncio.run(run_extraction())
