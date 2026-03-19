# encoding: utf-8
import os
from typing import List

from fastapi import Query, Path, HTTPException
from fastapi import Response
from pydantic import BaseModel
from sqlalchemy import select

from dbsession import async_session
from endpoints.get_virtual_chain_blue_score import current_blue_score_data
from models.Block import Block
from models.Transaction import Transaction, TransactionOutput, TransactionInput
from server import app, htnd_client

IS_SQL_DB_CONFIGURED = os.getenv("SQL_URI") is not None


class VerboseDataModel(BaseModel):
    hash: str = ""
    difficulty: float = 0
    selectedParentHash: str = ""
    transactionIds: List[str] | None = []
    blueScore: int = 0
    childrenHashes: List[str] | None = None
    mergeSetBluesHashes: List[str] = []
    mergeSetRedsHashes: List[str] = []
    isChainBlock: bool | None = None


class ParentHashModel(BaseModel):
    parentHashes: List[str] = []


class BlockHeader(BaseModel):
    version: int = 0
    hashMerkleRoot: str = ""
    acceptedIdMerkleRoot: str = ""
    utxoCommitment: str = ""
    timestamp: int = 0
    bits: int = 0
    nonce: str = ""
    daaScore: int = 0
    blueWork: str = ""
    parents: List[ParentHashModel]
    blueScore: int = 0
    pruningPoint: str = ""


class BlockModel(BaseModel):
    header: BlockHeader
    transactions: list | None
    verboseData: VerboseDataModel


class BlockResponse(BaseModel):
    blockHashes: List[str] = [""]
    blocks: List[BlockModel] | None


@app.get("/blocks/{blockId}", response_model=BlockModel, tags=["Hoosat blocks"])
async def get_block(response: Response,
                    blockId: str = Path(pattern="[a-f0-9]{64}")):
    """
    Get block information for a given block id
    """
    resp = await htnd_client.request("getBlockRequest",
                                       params={
                                           "hash": blockId,
                                           "includeTransactions": True
                                       })
    requested_block = None

    if "block" in resp["getBlockResponse"]:
        # We found the block in htnd. Just use it
        requested_block = resp["getBlockResponse"]["block"]
    else:
        if IS_SQL_DB_CONFIGURED:
            # Didn't find the block in htnd. Try getting it from the DB
            response.headers["X-Data-Source"] = "Database"
            requested_block = await get_block_from_db(blockId)

    if not requested_block:
        # Still did not get the block
        print("hier")
        raise HTTPException(status_code=404, detail="Block not found", headers={
            "Cache-Control": "public, max-age=1"
        })

    # We found the block, now we guarantee it contains the transactions
    # It's possible that the block from htnd does not contain transactions
    if 'transactions' not in requested_block or not requested_block['transactions']:
        requested_block['transactions'] = await get_block_transactions(blockId)

    if int(requested_block["header"]["blueScore"]) > current_blue_score_data["blue_score"] - 20:
        response.headers["Cache-Control"] = "public, max-age=1"

    elif int(requested_block["header"]["blueScore"]) > current_blue_score_data["blue_score"] - 60:
        response.headers["Cache-Control"] = "public, max-age=10"

    else:
        response.headers["Cache-Control"] = "public, max-age=600"

    return requested_block


@app.get("/blocks", response_model=BlockResponse, tags=["Hoosat blocks"])
async def get_blocks(response: Response,
                     lowHash: str = Query(pattern="[a-f0-9]{64}"),
                     includeBlocks: bool = False,
                     includeTransactions: bool = False):
    """
    Lists block beginning from a low hash (block id). Note that this function tries to determine the blocks from
    the htnd node. If this is not possible, the database is getting queryied as backup. In this case the response
    header contains the key value pair: x-data-source: database.

    Additionally the fields in verboseData: isChainBlock, childrenHashes and transactionIds can't be filled.
    """
    response.headers["Cache-Control"] = "public, max-age=3"

    resp = await htnd_client.request("getBlocksRequest",
                                       params={
                                           "lowHash": lowHash,
                                           "includeBlocks": includeBlocks,
                                           "includeTransactions": includeTransactions
                                       })

    return resp["getBlocksResponse"]


@app.get("/blocks-from-bluescore", response_model=List[BlockModel], tags=["Hoosat blocks"])
async def get_blocks_from_bluescore(response: Response,
                                    blueScore: int = 43679173,
                                    includeTransactions: bool = False):
    """
    Lists block beginning from a low hash (block id). Note that this function is running on a htnd and not returning
    data from database.
    """
    response.headers["X-Data-Source"] = "Database"

    if blueScore > current_blue_score_data["blue_score"] - 20:
        response.headers["Cache-Control"] = "no-store"

    blocks = await get_blocks_from_db_by_bluescore(blueScore)

    return [{
        "header": {
            "version": block.version,
            "hashMerkleRoot": block.hash_merkle_root,
            "acceptedIdMerkleRoot": block.accepted_id_merkle_root,
            "utxoCommitment": block.utxo_commitment,
            "timestamp": round(block.timestamp.timestamp() * 1000),
            "bits": block.bits,
            "nonce": block.nonce,
            "daaScore": block.daa_score,
            "blueWork": block.blue_work,
            "parents": [{"parentHashes": block.parents}],
            "blueScore": block.blue_score,
            "pruningPoint": block.pruning_point
        },
        "transactions": (txs := (await get_block_transactions(block.hash))) if includeTransactions else None,
        "verboseData": {
            "hash": block.hash,
            "difficulty": block.difficulty,
            "selectedParentHash": block.selected_parent_hash,
            "transactionIds": [tx["verboseData"]["transactionId"] for tx in txs] if includeTransactions else None,
            "blueScore": block.blue_score,
            "childrenHashes": None,
            "mergeSetBluesHashes": block.merge_set_blues_hashes,
            "mergeSetRedsHashes": block.merge_set_reds_hashes,
            "isChainBlock": None,
        }
    } for block in blocks]


@app.get("/blocks-range", response_model=List[BlockModel], tags=["Hoosat blocks"])
async def get_blocks_range(response: Response,
                           fromBlueScore: int = Query(..., description="Starting blueScore (inclusive)"),
                           toBlueScore: int = Query(..., description="Ending blueScore (inclusive)"),
                           includeTransactions: bool = False,
                           limit: int = Query(1000, le=5000, description="Max blocks to return")):
    """
    Get blocks within a blueScore range. Useful for indexing historical data.
    Returns blocks ordered by blueScore ascending.
    Maximum 5000 blocks per request without transactions.
    Maximum 100 blocks per request WITH transactions to prevent API overload.
    For faster lightweight queries with only coinbase payload, use /blocks-range-lightweight endpoint.
    """
    response.headers["X-Data-Source"] = "Database"

    # Validate range
    if fromBlueScore > toBlueScore:
        raise HTTPException(status_code=400, detail="fromBlueScore must be <= toBlueScore")

    # Strict limits when including transactions to prevent API overload
    max_range = 100 if includeTransactions else 10000
    max_limit = 100 if includeTransactions else 5000

    if toBlueScore - fromBlueScore > max_range:
        raise HTTPException(
            status_code=400,
            detail=f"Range too large. Maximum {max_range} blueScore difference allowed" +
                   (" with includeTransactions=true" if includeTransactions else "")
        )

    # Enforce limit based on includeTransactions
    if limit > max_limit:
        limit = max_limit

    blocks = await get_blocks_from_db_by_bluescore_range(fromBlueScore, toBlueScore, limit)

    # Set cache headers based on how recent the data is
    if toBlueScore > current_blue_score_data["blue_score"] - 20:
        response.headers["Cache-Control"] = "no-store"
    elif toBlueScore > current_blue_score_data["blue_score"] - 100:
        response.headers["Cache-Control"] = "public, max-age=10"
    else:
        response.headers["Cache-Control"] = "public, max-age=600"

    # Optimize transaction loading for multiple blocks
    if includeTransactions and blocks:
        # Get all transactions for all blocks in one query
        block_hashes = [block.hash for block in blocks]
        all_transactions = await get_transactions_for_multiple_blocks(block_hashes)
        transactions_by_block = {block_hash: txs for block_hash, txs in all_transactions.items()}
    else:
        transactions_by_block = {}

    return [{
        "header": {
            "version": block.version,
            "hashMerkleRoot": block.hash_merkle_root,
            "acceptedIdMerkleRoot": block.accepted_id_merkle_root,
            "utxoCommitment": block.utxo_commitment,
            "timestamp": round(block.timestamp.timestamp() * 1000),
            "bits": block.bits,
            "nonce": block.nonce,
            "daaScore": block.daa_score,
            "blueWork": block.blue_work,
            "parents": [{"parentHashes": block.parents}],
            "blueScore": block.blue_score,
            "pruningPoint": block.pruning_point
        },
        "transactions": transactions_by_block.get(block.hash, []) if includeTransactions else None,
        "verboseData": {
            "hash": block.hash,
            "difficulty": block.difficulty,
            "selectedParentHash": block.selected_parent_hash,
            "transactionIds": [tx["verboseData"]["transactionId"] for tx in
                               transactions_by_block.get(block.hash, [])] if includeTransactions else None,
            "blueScore": block.blue_score,
            "childrenHashes": None,
            "mergeSetBluesHashes": block.merge_set_blues_hashes,
            "mergeSetRedsHashes": block.merge_set_reds_hashes,
            "isChainBlock": None,
        }
    } for block in blocks]


@app.get("/blocks-range-lightweight", response_model=List[dict], tags=["Hoosat blocks"])
async def get_blocks_range_lightweight(response: Response,
                                       fromBlueScore: int = Query(...),
                                       toBlueScore: int = Query(...),
                                       limit: int = Query(1000, le=5000)):
    """
    Lightweight endpoint returning blocks with ALL transaction payloads (not just coinbase).
    In HTN, any user can include payload in their transaction for voting (KIP-14).
    Much faster than full blocks-range with complete transaction data.
    Returns transaction IDs with payloads to track voting participants.
    """
    response.headers["X-Data-Source"] = "Database"

    # Validate range
    if fromBlueScore > toBlueScore:
        raise HTTPException(status_code=400, detail="fromBlueScore must be <= toBlueScore")

    if toBlueScore - fromBlueScore > 10000:
        raise HTTPException(status_code=400,
                            detail="Range too large. Maximum 10000 blueScore difference allowed")

    blocks = await get_blocks_from_db_by_bluescore_range(fromBlueScore, toBlueScore, limit)

    # Set cache headers
    if toBlueScore > current_blue_score_data["blue_score"] - 20:
        response.headers["Cache-Control"] = "no-store"
    elif toBlueScore > current_blue_score_data["blue_score"] - 100:
        response.headers["Cache-Control"] = "public, max-age=10"
    else:
        response.headers["Cache-Control"] = "public, max-age=600"

    block_hashes = [block.hash for block in blocks]
    all_payloads = await get_all_payloads_batch(block_hashes)

    return [{
        "blockHash": block.hash,
        "blueScore": block.blue_score,
        "timestamp": round(block.timestamp.timestamp() * 1000),
        "difficulty": block.difficulty,
        "payloads": all_payloads.get(block.hash, [])
    } for block in blocks]


async def get_all_payloads_batch(block_hashes):
    """
    Get ALL transaction payloads for multiple blocks (not just coinbase).
    In HTN/Kaspa, any transaction can include payload for voting (KIP-14).
    Returns transaction ID with payload to track who voted.
    """
    if not block_hashes:
        return {}

    async with async_session() as s:
        # Get ALL transactions with payload (remove subnetwork_id filter)
        transactions = await s.execute(
            select(Transaction.block_hash, Transaction.transaction_id, Transaction.payload)
            .where(Transaction.block_hash.overlap(block_hashes))
            .where(Transaction.payload.isnot(None))
            .where(Transaction.payload != '')
        )

        result = {}
        for tx in transactions:
            # Each block can be in multiple block_hash entries (DAG structure)
            for block_hash in tx.block_hash:
                if block_hash in block_hashes:
                    if block_hash not in result:
                        result[block_hash] = []
                    result[block_hash].append({
                        'txId': tx.transaction_id,
                        'payload': tx.payload
                    })

        return result


async def get_blocks_from_db_by_bluescore(blue_score):
    async with async_session() as s:
        blocks = (await s.execute(select(Block)
                                  .where(Block.blue_score == blue_score))).scalars().all()

    return blocks


async def get_blocks_from_db_by_bluescore_range(from_score: int, to_score: int, limit: int):
    """
    Get blocks within a blueScore range from the database
    """
    async with async_session() as s:
        blocks = (await s.execute(
            select(Block)
            .where(Block.blue_score >= from_score)
            .where(Block.blue_score <= to_score)
            .order_by(Block.blue_score.asc())
            .limit(limit)
        )).scalars().all()

    return blocks


async def get_block_from_db(blockId):
    """
    Get the block from the database
    """
    async with async_session() as s:
        requested_block = await s.execute(select(Block)
                                          .where(Block.hash == blockId).limit(1))

        try:
            requested_block = requested_block.first()[0]  # type: Block
        except TypeError:
            raise HTTPException(status_code=404, detail="Block not found", headers={
                "Cache-Control": "public, max-age=3"
            })

    if requested_block:
        return {
            "header": {
                "version": requested_block.version,
                "hashMerkleRoot": requested_block.hash_merkle_root,
                "acceptedIdMerkleRoot": requested_block.accepted_id_merkle_root,
                "utxoCommitment": requested_block.utxo_commitment,
                "timestamp": round(requested_block.timestamp.timestamp() * 1000),
                "bits": requested_block.bits,
                "nonce": requested_block.nonce,
                "daaScore": requested_block.daa_score,
                "blueWork": requested_block.blue_work,
                "parents": [{"parentHashes": requested_block.parents}],
                "blueScore": requested_block.blue_score,
                "pruningPoint": requested_block.pruning_point
            },
            "transactions": None,  # This will be filled later
            "verboseData": {
                "hash": requested_block.hash,
                "difficulty": requested_block.difficulty,
                "selectedParentHash": requested_block.selected_parent_hash,
                "transactionIds": None,  # information not in database
                "blueScore": requested_block.blue_score,
                "childrenHashes": None,  # information not in database
                "mergeSetBluesHashes": requested_block.merge_set_blues_hashes,
                "mergeSetRedsHashes": requested_block.merge_set_reds_hashes,
                "isChainBlock": None,  # information not in database
            }
        }
    return None


"""
Get the transactions associated with a block
"""


async def get_block_transactions(blockId):
    # create tx data
    tx_list = []

    async with async_session() as s:
        transactions = await s.execute(select(Transaction).filter(Transaction.block_hash.contains([blockId])))

        transactions = transactions.scalars().all()

        tx_outputs = await s.execute(select(TransactionOutput)
                                     .where(TransactionOutput.transaction_id
                                            .in_([tx.transaction_id for tx in transactions])))

        tx_outputs = tx_outputs.scalars().all()

        tx_inputs = await s.execute(select(TransactionInput)
                                    .where(TransactionInput.transaction_id
                                           .in_([tx.transaction_id for tx in transactions])))

        tx_inputs = tx_inputs.scalars().all()

    for tx in transactions:
        tx_list.append({
            "inputs": [
                {
                    "previousOutpoint": {
                        "transactionId": tx_inp.previous_outpoint_hash,
                        "index": tx_inp.previous_outpoint_index
                    },
                    "signatureScript": tx_inp.signature_script,
                    "sigOpCount": tx_inp.sig_op_count
                }
                for tx_inp in tx_inputs if tx_inp.transaction_id == tx.transaction_id],
            "outputs": [
                {
                    "amount": tx_out.amount,
                    "scriptPublicKey": {
                        "scriptPublicKey": tx_out.script_public_key
                    },
                    "verboseData": {
                        "scriptPublicKeyType": tx_out.script_public_key_type,
                        "scriptPublicKeyAddress": tx_out.script_public_key_address
                    }
                } for tx_out in tx_outputs if tx_out.transaction_id == tx.transaction_id],
            "subnetworkId": tx.subnetwork_id,
            "verboseData": {
                "transactionId": tx.transaction_id,
                "hash": tx.hash,
                "mass": tx.mass,
                "blockHash": tx.block_hash,
                "blockTime": tx.block_time
            }
        })

    return tx_list


async def get_transactions_for_multiple_blocks(block_hashes):
    """
    Optimized function to get transactions for multiple blocks in batch
    Reduces N+1 query problem by loading all data in fewer queries
    """
    if not block_hashes:
        return {}
    
    async with async_session() as s:
        # Get all transactions for all blocks in one query
        transactions = await s.execute(
            select(Transaction)
            .where(Transaction.block_hash.overlap(block_hashes))
        )
        transactions = transactions.scalars().all()
        
        if not transactions:
            return {block_hash: [] for block_hash in block_hashes}
        
        # Group transactions by block hash
        transactions_by_block = {}
        for tx in transactions:
            for block_hash in tx.block_hash:
                if block_hash in block_hashes:
                    if block_hash not in transactions_by_block:
                        transactions_by_block[block_hash] = []
                    transactions_by_block[block_hash].append(tx)
        
        # Get all transaction IDs
        all_tx_ids = [tx.transaction_id for tx in transactions]
        
        # Get all outputs in one query
        tx_outputs = await s.execute(
            select(TransactionOutput)
            .where(TransactionOutput.transaction_id.in_(all_tx_ids))
        )
        tx_outputs = tx_outputs.scalars().all()
        
        # Get all inputs in one query
        tx_inputs = await s.execute(
            select(TransactionInput)
            .where(TransactionInput.transaction_id.in_(all_tx_ids))
        )
        tx_inputs = tx_inputs.scalars().all()
        
        # Group outputs and inputs by transaction ID
        outputs_by_tx = {}
        for output in tx_outputs:
            if output.transaction_id not in outputs_by_tx:
                outputs_by_tx[output.transaction_id] = []
            outputs_by_tx[output.transaction_id].append(output)
        
        inputs_by_tx = {}
        for input_tx in tx_inputs:
            if input_tx.transaction_id not in inputs_by_tx:
                inputs_by_tx[input_tx.transaction_id] = []
            inputs_by_tx[input_tx.transaction_id].append(input_tx)
        
        # Build the final result
        result = {}
        for block_hash, block_transactions in transactions_by_block.items():
            block_tx_list = []
            for tx in block_transactions:
                tx_data = {
                    "verboseData": {
                        "transactionId": tx.transaction_id,
                        "hash": tx.hash,
                        "mass": tx.mass,
                        "blockHash": block_hash,
                        "blockTime": tx.block_time,
                        "isAccepted": tx.is_accepted,
                        "acceptingBlockHash": tx.accepting_block_hash
                    },
                    "subnetworkId": tx.subnetwork_id,
                    "transactionId": tx.transaction_id,
                    "hash": tx.hash,
                    "mass": tx.mass,
                    "blockHash": block_hash,
                    "blockTime": tx.block_time,
                    "isAccepted": tx.is_accepted,
                    "acceptingBlockHash": tx.accepting_block_hash,
                    "outputs": [
                        {
                            "id": output.id,
                            "transactionId": output.transaction_id,
                            "index": output.index,
                            "amount": output.amount,
                            "scriptPublicKey": output.script_public_key,
                            "scriptPublicKeyAddress": output.script_public_key_address,
                            "scriptPublicKeyType": output.script_public_key_type,
                            "acceptingBlockHash": output.accepting_block_hash
                        }
                        for output in outputs_by_tx.get(tx.transaction_id, [])
                    ],
                    "inputs": [
                        {
                            "id": input_tx.id,
                            "transactionId": input_tx.transaction_id,
                            "index": input_tx.index,
                            "previousOutpointHash": input_tx.previous_outpoint_hash,
                            "previousOutpointIndex": input_tx.previous_outpoint_index,
                            "signatureScript": input_tx.signature_script,
                            "sigOpCount": input_tx.sig_op_count
                        }
                        for input_tx in inputs_by_tx.get(tx.transaction_id, [])
                    ]
                }
                block_tx_list.append(tx_data)
            result[block_hash] = block_tx_list
        
        # Ensure all requested blocks have an entry (even if empty)
        for block_hash in block_hashes:
            if block_hash not in result:
                result[block_hash] = []
        
        return result
