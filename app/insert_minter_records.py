from datetime import time

from web3 import Web3
import pandas as pd
import psycopg2
import os
from psycopg2 import OperationalError
from app.common.common_utils import save_data_to_csv

# Setup Web3 connection with Infura/Alchemy (Mainnet required for ENS)
web3 = Web3(Web3.HTTPProvider(os.getenv("INFURA_URL")))


def insert_mintres_records(contract_address):
    print("Inside the insert_mintres_records function")
    print("Inserting minter records...")
    try:
        conn = psycopg2.connect(os.getenv("DATA_BASE"))
        cur = conn.cursor()
        cur.execute("""
                    INSERT INTO minters (minter_address, total_mint_value, avg_mints_per_contract, cross_contract_count, activity_frequency)
                    SELECT
                        e.to_address AS minter_address,
                        COALESCE(SUM(t.value) / 1e18, 0) AS total_mint_value,  -- Convert from Wei to ETH
                        COUNT(*) * 1.0 / COUNT(DISTINCT e.contract_address) AS avg_mints_per_contract,
                        COUNT(DISTINCT e.contract_address) AS cross_contract_count,
                        COUNT(*) * 1.0 / COUNT(DISTINCT DATE(e.timestamp)) AS activity_frequency
                    FROM nft_events e
                    LEFT JOIN transactions t ON e.tx_hash = t.tx_hash AND e.contract_address = t.contract_address
                    WHERE e.event_type = 'Mint'
                    GROUP BY e.to_address
                    ON CONFLICT (minter_address) DO UPDATE SET
                        total_mint_value = EXCLUDED.total_mint_value,
                        avg_mints_per_contract = EXCLUDED.avg_mints_per_contract,
                        cross_contract_count = EXCLUDED.cross_contract_count,
                        activity_frequency = EXCLUDED.activity_frequency;
                    """
                    )

        # rows = cur.fetchall()
        # print("records : ", len(rows))
        conn.commit()
        cur.close()
        conn.close()
        insert_eth_name()
        return {"message": "✅ Minter record inserted successfully"}
    except Exception as e:
        print("Exception : ",e)


def insert_eth_name():
    print("Inside the insert_eth_name function")
    # Check if ENS is enabled
    if not web3.ens:
        from ens import ENS
        ns = ENS.fromWeb3(web3)
        web3.ens = ns

    conn = psycopg2.connect(os.getenv("DATA_BASE"))
    cur = conn.cursor()
    conn.autocommit = True
    # Fetch all Ethereum addresses
    cur.execute("SELECT minter_id, minter_address FROM minters WHERE eth_domain IS NULL;")
    rows = cur.fetchall()

    df = pd.DataFrame(columns=['minter_id', 'minter_address', 'eth_domain','is_known_account'])
    print(f"🔍 Checking {len(rows)} addresses...")
    count = 1
    for minter_id, address in rows:
        try:
            ens_name = web3.ens.name(address.lower())  # Reverse lookup
            if ens_name:
                print(f"✅ {count}. {minter_id}, {address} → {ens_name}")
                try:
                    cur.execute(
                        "UPDATE minters SET eth_domain = %s, is_known_account =%s WHERE minter_id = %s",
                        (ens_name, True, minter_id)
                    )
                    conn.commit()
                except OperationalError as e:
                    print(f"❌ Database error: {e}")
                count = count+1
                # if count ==10:
                #     break
            else:
                print(f"❌ {minter_id}, {address} -> No ENS name")
        except Exception as e:
            print(f"⚠️ Error for {address}: {e}")
    cur.close()
    conn.close()
    print("🎉 ENS update completed.")
    print(f"Out of{len(rows)} address, {count} address Eth name inserted.")
