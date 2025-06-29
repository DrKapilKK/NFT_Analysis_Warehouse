from web3 import Web3
from datetime import datetime
from dotenv import load_dotenv
import os
from app.common.common_utils import get_contract_creation_block

# Replace with your Infura project URL (for Mainnet)
load_dotenv()
INFURA_URL = os.getenv("INFURA_URL")
CONTRACT_ADDRESS = Web3.to_checksum_address(os.getenv("CONTRACT_ADDRESS"))
web3 = Web3(Web3.HTTPProvider(INFURA_URL))

def get_contract_details(contract_address,is_known_collection):
    print(contract_address)
    if not Web3.is_address(contract_address):
        raise ValueError("Invalid Ethereum address format")

    contract_address = Web3.to_checksum_address(contract_address)

    # 1. Fetch contract creation transaction
    code = web3.eth.get_code(contract_address)
    if not code or code == b'0x':
        raise Exception("No contract found at this address")

    # 2. Try to get contract name (ERC721 or ERC20)
    abi_name_check = [
        {
            "constant": True,
            "inputs": [],
            "name": "name",
            "outputs": [{"name": "", "type": "string"}],
            "payable": False,
            "stateMutability": "view",
            "type": "function",
        }
    ]
    contract = web3.eth.contract(address=contract_address, abi=abi_name_check)
    try:
        contract_name = contract.functions.name().call()
    except:
        contract_name = "Unknown"

    # 3. Estimate mint_live_date from creation block
    try:
        creation_block = get_contract_creation_block(contract_address)
        block = web3.eth.get_block(creation_block)
        mint_live_date = datetime.utcfromtimestamp(block.timestamp).isoformat() + 'Z'
    except Exception:
        mint_live_date = None

    # 4. is_known_collection is always False here (needs external data)
    return {
        "contract_address": contract_address,
        "contract_name": contract_name,
        "mint_live_date": mint_live_date,
        "is_known_collection": is_known_collection # Placeholder, can't detect on-chain
    }




# Example usage
# if __name__ == "__main__":
#     # contract_address = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85"
#     try:
#         details = get_contract_details(CONTRACT_ADDRESS,True)
#         print("Contract Details:")
#         for key, value in details.items():
#             print(f"{key}: {value}")
#     except Exception as e:
#         print("Error:", e)
