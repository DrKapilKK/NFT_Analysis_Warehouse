from venv import logger
from fastapi import FastAPI, HTTPException

import psycopg2
from app.contract_details import get_contract_details
from app.get_all_transfer_mint_tx import store_transfer_and_mint_events
from app.insert_minter_records import insert_mintres_records
from web3 import Web3
import os
import pandas as pd


app = FastAPI()


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
        return new_contract_id
    except Exception as e:
        print("Error while inserting contract details into table",e)
        return e

#Done contract_detail.19-06-25
def contract_detail(contract_address):
    logger.info("Inside the contract_detail function")
    try:
        details = get_contract_details(contract_address,True)
        db_result = insert_contract_detail_to_db(details)
        return db_result
    except Exception as e:
        print("Error:", e)
        return e

def format_address(hex_str):
    return "0x" + hex_str[-40:].lower()
# @app.get("/home")
def main():
    # status = {}
    # result = contract_detail(CONTRACT_ADDRESS)
    # status["status"] = result
    # return status["status"]
    NFT_CONTRACTS = os.getenv("CONTRACT_ADDRESS")
    if not NFT_CONTRACTS:
        raise HTTPException(status_code=400, detail="Missing CONTRACT_ADDRESS in environment")
    result = insert_mintres_records(NFT_CONTRACTS)
    print(result)
    return {"status": "minters added"}



@app.post("/fetch_all_data/", status_code=201)
def fetch_all_data():
    NFT_CONTRACTS = os.getenv("NFT_CONTRACTS")
    store_transfer_and_mint_events(NFT_CONTRACTS,1000)


# @app.post("/app/add_minters/", status_code=201)
# def add_minters():
#     NFT_CONTRACTS = os.getenv("CONTRACT_ADDRESS")
#     if not NFT_CONTRACTS:
#         raise HTTPException(status_code=400, detail="Missing CONTRACT_ADDRESS in environment")
#     insert_mintres_records(NFT_CONTRACTS)
#     return {"status": "minters added"}


if __name__ == "__main__":
    main()