import time
from web3 import Web3
import psycopg2
import os
import json
from dotenv import load_dotenv
import requests
from app.common.common_utils import get_contract_creation_block,safe_get_transaction

load_dotenv()

INFURA_URL = os.getenv("INFURA_URL")
POSTGRES_URL = os.getenv("DATA_BASE")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'
url = f"https://api.etherscan.io/api"

web3 = Web3(Web3.HTTPProvider(INFURA_URL))
conn = psycopg2.connect(POSTGRES_URL)
cur = conn.cursor()
TRANSFER_EVENT_SIG = web3.keccak(text="Transfer(address,address,uint256)").hex()
TRANSFER_EVENT_SIG = "0x" + TRANSFER_EVENT_SIG


def store_transfer_and_mint_events(contract_info, batch_size=1000):
    print(contract_info)
    print("Connected:", web3.is_connected())
    contract_address = Web3.to_checksum_address(contract_info)
    #get ABI
    # with open(contract_info["abi_path"]) as f:
    #     abi = json.load(f)
    params = {
        "module": "contract",
        "action": "getabi",
        "address": contract_info,
        "apikey": ETHERSCAN_API_KEY
    }

    response = requests.get(url, params=params)
    abi = json.loads(response.json()['result'])

    contract = web3.eth.contract(address=contract_address, abi=abi)
    print("contract is :",contract.address)
    # STEP 1: Get START BLOCK
    cur.execute("""
        SELECT MAX(block_number) FROM nft_events
        WHERE contract_address = %s
    """, (contract_address,))
    result = cur.fetchone()
    last_stored_block = result[0]

    if last_stored_block:
        from_block = last_stored_block
        print(f"📦 Resuming from block {from_block} for {contract_address}")
    else:
        try:
            # tx_receipt = web3.eth.get_transaction_receipt(contract_address)
            from_block = get_contract_creation_block(web3, contract_address)
            print(f"🆕 Starting from deployment block {from_block} for {contract_address}")
        except Exception as e:
            print(f"⚠️ Error fetching contract deployment block: {e}")
            return

    latest_block = web3.eth.block_number

    # STEP 2: Fetch logs in batches
    while from_block <= latest_block:
        to_block = min(from_block + batch_size - 1, latest_block)
        print(f"🔄 Fetching logs from block {from_block} to {to_block}")

        try:
            # logs = contract.events.Transfer().get_logs(
            #     filter_params={"fromBlock": from_block, "toBlock": to_block}
            # )
            logs = web3.eth.get_logs({
                "fromBlock": from_block,
                "toBlock": to_block,
                "address": contract_address,
                "topics": [TRANSFER_EVENT_SIG]
            })
            print(f"Fetched {len(logs)} logs.")

        except Exception as e:
            print(f"⚠️ Error fetching logs: {e}")
            from_block += batch_size
            continue

        print(f"  ⛓️ {len(logs)} logs found")
        count = 1

        for log in logs:
            try:
                decoded_log = contract.events.Transfer().process_log(log)
                # print("decoded_log ", decoded_log)
                # time.sleep(0.2)
            except Exception as e:
                print(f"❌ Failed to decode log: {e}")
                continue
            tx_hash = decoded_log.transactionHash.hex()
            token_id = str(decoded_log.args.tokenId)
            from_addr = decoded_log.args["from"]
            to_addr = decoded_log.args["to"]
            block_number = decoded_log.blockNumber
            log_index = decoded_log.logIndex
            timestamp = web3.eth.get_block(block_number).timestamp

            try:
                tx = web3.eth.get_transaction(log.transactionHash)
                # tx = safe_get_transaction(web3, log.transactionHash)
                receipt = web3.eth.get_transaction_receipt(log.transactionHash)
                # print("receipt", receipt)
            except Exception as e:
                if "429" in str(e):
                    continue
                print("Exeption occured while getting transaction from log hash.")

            # Insert transaction
            try:
                cur.execute("""
                    INSERT INTO transactions (
                        tx_hash, contract_address, block_number, from_address, to_address,
                        value, gas_used, gas_price, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))
                    ON CONFLICT (tx_hash, contract_address) DO NOTHING
                """, (
                    tx_hash, contract_address, block_number, tx["from"], tx["to"],
                    tx["value"], receipt.gasUsed, tx.gasPrice, timestamp
                ))

                # Event type
                event_type = "Mint" if from_addr.lower() == ZERO_ADDRESS else "Transfer"

                # Insert event
                cur.execute("""
                    INSERT INTO nft_events (
                        contract_address, tx_hash, block_number, event_type, token_id,
                        from_address, to_address, log_index, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))
                """, (
                    contract_address, tx_hash, block_number, event_type, token_id,
                    from_addr, to_addr, log_index, timestamp
                ))
            except Exception as e:
                print("Exception occurred at insertion:", e)
            print(f"logs: {len(logs)}, Record added : {count} and log is {decoded_log}")
            count = count + 1

        conn.commit()
        print(f"  ✅ Synced batch up to block {to_block}")
        from_block += batch_size