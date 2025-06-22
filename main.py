from venv import logger

import psycopg2
import requests
import datetime
import csv
import json
import time
from eth_utils import keccak
# from nft_transfer import nft_transfer_data_scrap
from app.contract_details import get_contract_details
# from get_minters_from_contract import get_minters_details_from_contract
from common.common_utils import save_data_to_csv
from web3 import Web3
import os


CONTRACT_ADDRESS = Web3.to_checksum_address(os.getenv("CONTRACT_ADDRESS"))
INFURA_URL = os.getenv("INFURA_URL")
DB_URL = os.getenv("DATA_BASE")

def insert_contract_detail_to_db(details):
    logger.info("Inside the insert_contract_detail_to_db function.")
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        insert_query = """
            INSERT INTO NFT_Contracts (
                contract_address,
                contract_name,
                mint_live_date,
                is_known_collection
            )
            VALUES (%s, %s, %s, %s)
            RETURNING contract_id;
        """
        cur.execute(insert_query,(details["contract_address"], details["contract_name"],details["mint_live_date"],details["is_known_collection"]))
        # Fetch the generated contract_id
        new_contract_id = cur.fetchone()[0]
        print(f"Inserted record with contract_id: {new_contract_id}")
        conn.commit()
        print(f"✅ Inserted contract details into nft_contracts table of PostgreSQL.")
        cur.close()
        conn.close()
    except Exception as e:
        print("Error while inserting contract details into table",e)

#Done contract_detail.19-06-25
def contract_detail(contract_address):
    logger.info("Inside the contract_detail function")
    try:
        details = get_contract_details(contract_address,True)
        insert_contract_detail_to_db(details)
    except Exception as e:
        print("Error:", e)

def format_address(hex_str):
    return "0x" + hex_str[-40:].lower()

def main():
    contract_detail(CONTRACT_ADDRESS)


if __name__ == "__main__":
    main()