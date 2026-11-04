# encoding: utf-8

import logging
import os
from pydantic import BaseModel

from helper.deflationary_table import DEFLATIONARY_TABLE
from server import app, htnd_client

_logger = logging.getLogger(__name__)

# On mainnet HTND pays 5 % of each block's reward to the dev-fee address from
# DAA score 17,500,000 on (block version 2). The hard fork of block version 12
# (mainnet DAA 258,000,000, HTND 2.18.4) removes that output, so the miner gets
# the whole reward. The end is a setting for testnet, whose version-13 fork at
# DAA 10,000,000 removes it there.
DEV_FEE_SHARE = 0.05
DEV_FEE_START_DAA_SCORE = 17_500_000
DEFAULT_DEV_FEE_END_DAA_SCORE = 258_000_000


def _dev_fee_end_daa_score() -> int:
    """DEV_FEE_END_DAA_SCORE from the environment; the mainnet fork's score when unset or not a positive integer."""
    value = os.getenv("DEV_FEE_END_DAA_SCORE")
    if value is None or not value.strip():
        return DEFAULT_DEV_FEE_END_DAA_SCORE
    value = value.strip()
    if value.isascii() and value.isdigit() and int(value) > 0:
        return int(value)
    _logger.warning(f"DEV_FEE_END_DAA_SCORE={value!r} is not a positive integer; "
                    f"using {DEFAULT_DEV_FEE_END_DAA_SCORE}")
    return DEFAULT_DEV_FEE_END_DAA_SCORE


DEV_FEE_END_DAA_SCORE = _dev_fee_end_daa_score()


class BlockRewardResponse(BaseModel):
    blockreward: float = 13.333333332
    minerreward: float = 13.333333332
    devreward: float = 0


@app.get("/info/blockreward", response_model=BlockRewardResponse | str, tags=["Hoosat network info"])
async def get_blockreward(stringOnly: bool = False):
    """
    Returns the current blockreward in HTN/block and how it is split. From DAA score 17,500,000 the dev fee takes
    5 % of it (minerreward = 95 %, devreward = 5 %); the hard fork of block version 12 at DAA score 258,000,000
    removes the dev fee, so from then on minerreward is the whole blockreward and devreward is 0.
    """
    resp = await htnd_client.request("getBlockDagInfoRequest")
    if resp is  None: 
        return "Request getBlockDagInfoRequest failed"
    daa_score = int(resp["getBlockDagInfoResponse"]["virtualDaaScore"])

    reward = 0

    for to_break_score in sorted(DEFLATIONARY_TABLE):
        reward = DEFLATIONARY_TABLE[to_break_score]
        if daa_score < to_break_score:
            break

    
    bps = int(os.getenv("BPS", "5"))
    reward = reward / bps

    if DEV_FEE_START_DAA_SCORE <= daa_score < DEV_FEE_END_DAA_SCORE:
        minerreward, devreward = reward * (1 - DEV_FEE_SHARE), reward * DEV_FEE_SHARE
    else:
        minerreward, devreward = reward, 0.0

    if not stringOnly:
        return {
            "blockreward": reward,
            "minerreward": minerreward,
            "devreward": devreward
        }

    else:
        return f"{reward:.2f}\n{minerreward}\n{devreward}"


