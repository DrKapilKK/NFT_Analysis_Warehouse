from venv import logger
import csv
import os

import time
import requests
from web3 import Web3
from web3.exceptions import TransactionNotFound

INFURA_URL = os.environ.get("INFURA_URL")
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
def save_data_to_csv(data, filename='data.csv'):
    if not data:
        print("No data to save.")
        return

    # Extract column headers from keys of the first dictionary
    headers = list(data[0].keys())

    # Write to CSV
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

    print(f"Saved {len(data)} records to '{filename}'")

def get_contract_creation_block(web3,address):
    """
    Estimate the creation block by binary search over blocks.
    WARNING: This can be slow on mainnet without access to archive node.
    """
    logger.info("Inside get_contract_creation_block!!!")
    print("Inside get_contract_creation_block!!!")
    start = 0
    end = web3.eth.block_number
    try:
        while start <= end:
            mid = (start + end) // 2
            code = web3.eth.get_code(address, block_identifier=mid)
            if code == b'' or code == b'0x':
                start = mid + 1
            else:
                end = mid - 1
        print("get_contract_creation_block give the deployed block is : ", start)
        return start
    except Exception as e:
        logger.error("Exception occurred the get_contract_creation_block",e)

from web3.exceptions import TransactionNotFound

def safe_get_transaction(web3, tx_hash, retries=5, delay=2):
    for attempt in range(retries):
        try:
            return web3.eth.get_transaction(tx_hash)
        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                print(f"Rate limit hit. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                raise
    raise Exception("Too many rate-limit errors.")