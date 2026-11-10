# encoding: utf-8
import csv
from io import StringIO
import logging
from fastapi import Path, HTTPException, Query
from fastapi.responses import StreamingResponse, Response
from pydantic import BaseModel
from typing import List     
from sqlalchemy import asc, desc, func, text
from sqlalchemy.future import select

from dbsession import async_session
from endpoints import filter_fields, sql_db_only
from models.Balance import Balance
from models.BalanceYesterday import BalanceYesterday
from models.AddressTag import AddressTag

from server import app, htnd_client


class BalanceResponse(BaseModel):
    address: str = "hoosat:pzhh76qc82wzduvsrd9xh4zde9qhp0xc8rl7qu2mvl2e42uvdqt75zrcgpm00"
    balance: int = 38240000000


class BalanceResponses(BaseModel):
    balances: List[BalanceResponse]


def csv_generator(balances):
    try:
        yield "Address,Balance\n"
        for balance in balances:
            yield f"{balance.script_public_key_address},{balance.balance}\n"
    except GeneratorExit:
        logging.info("Client disconnected during CSV streaming.")

@app.get("/addresses/{hoosatAddress}/balance", response_model=BalanceResponse, tags=["Hoosat addresses"])
async def get_balance_from_hoosat_address(
        hoosatAddress: str = Path(
            description="Hoosat address as string e.g. hoosat:pzhh76qc82wzduvsrd9xh4zde9qhp0xc8rl7qu2mvl2e42uvdqt75zrcgpm00",
            pattern="^hoosat\:[a-z0-9]{61,63}$")):
    """
    Get balance for a given hoosat address
    """
    resp = await htnd_client.request("getBalanceByAddressRequest",
                                       params={
                                           "address": hoosatAddress
                                       })

    try:
        if resp is not None:
            resp = resp["getBalanceByAddressResponse"]
    except KeyError:
        if resp is not None:
            if "getUtxosByAddressesResponse" in resp and "error" in resp["getUtxosByAddressesResponse"]:
                raise HTTPException(status_code=400, detail=resp["getUtxosByAddressesResponse"]["error"])
            else:
                raise
    balance = 0
    try:
        if resp is not None:
            balance = int(resp["balance"])
    except KeyError:
        balance = 0

    return {
        "address": hoosatAddress,
        "balance": balance
    }



@app.get("/addresses/balances/json", response_model=BalanceResponses, tags=["Hoosat addresses"])
async def get_balances():
    """
    Get balances of addresses.
    """
    async with async_session() as session:  
        try:
            result = await session.execute(
                select(Balance)
                .order_by(desc(Balance.balance))
            )
            balances = result.scalars().all()
            return BalanceResponses(balances=[
                BalanceResponse(address=balance.script_public_key_address, balance=balance.balance)
                for balance in balances
            ])
        except Exception as e:
            return BalanceResponses(balances=[])



@app.get("/addresses/balances/csv", response_class=StreamingResponse, tags=["Hoosat addresses"])
async def get_balances_csv():
    """
    Get balances of addresses in CSV format.
    """
    async with async_session() as session:
        try:
            result = await session.execute(select(Balance).order_by(desc(Balance.balance)))
            balances = result.scalars().all()
            return StreamingResponse(
                csv_generator(balances),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=balances.csv"}
            )
        except Exception as e:
            logging.exception("Error generating CSV:")
            return StreamingResponse(
                iter(["Address,Balance\n"]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=balances.csv"}
            )

@app.get("/addresses/balances/json/paged", response_model=BalanceResponses, tags=["Hoosat addresses"])
async def get_balances_paged(page: int = Query(1, ge=1), items_per_page: int = Query(10, ge=1)):
    """
    Get balances of addresses with pagination.
    """
    async with async_session() as session:
        try:
            result = await session.execute(
                select(Balance)
                .offset((page - 1) * items_per_page)
                .limit(items_per_page)
                .order_by(desc(Balance.balance))
            )
            balances = result.scalars().all()
            
            return BalanceResponses(
                balances=[
                    BalanceResponse(address=balance.script_public_key_address, balance=balance.balance)
                    for balance in balances
                ]
            )
        except Exception as e:
            return BalanceResponses(balances=[])


@app.get("/addresses/balances/csv/paged", response_class=StreamingResponse, tags=["Hoosat addresses"])
async def get_balances_csv_paged(page: int = Query(1, ge=1), items_per_page: int = Query(10, ge=1)):
    """
    Get balances of addresses in CSV format with pagination.
    """
    async with async_session() as session:
        try:
            result = await session.execute(
                select(Balance)
                .offset((page - 1) * items_per_page)
                .limit(items_per_page)
                .order_by(desc(Balance.balance))
            )
            balances = result.scalars().all()
            return StreamingResponse(
                csv_generator(balances),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=balances.csv"}
            )
        except Exception as e:
            logging.exception("Error generating paged CSV:")
            return StreamingResponse(
                iter(["Address,Balance\n"]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=balances.csv"}
            )

@app.get("/addresses/balances/csv/paged-with-yesterday", tags=["Hoosat addresses"])
async def get_balances_csv_paged_with_yesterday(page: int = Query(1, ge=1),
                                                items_per_page: int = Query(10, ge=1, le=100)):
    """
    One page of balances (largest first) with each address's balance from the last daily snapshot,
    for the explorer's 24h-change column. Columns: Address,Balance,YesterdayBalance (empty if not in the snapshot).
    """
    async with async_session() as session:
        result = await session.execute(
            select(Balance.script_public_key_address, Balance.balance, BalanceYesterday.balance)
            .outerjoin(BalanceYesterday,
                       BalanceYesterday.script_public_key_address == Balance.script_public_key_address)
            .order_by(desc(Balance.balance), Balance.script_public_key_address)
            .offset((page - 1) * items_per_page)
            .limit(items_per_page)
        )
        lines = ["Address,Balance,YesterdayBalance"]
        for address, balance, yesterday in result.all():
            lines.append(f"{address},{balance},{'' if yesterday is None else yesterday}")
        return Response("\n".join(lines) + "\n", media_type="text/csv")


_SUMMARY_SQL = text("""
    with r as (
        select balance, row_number() over (order by balance desc) as idx
        from balances_yesterday
    )
    select
        coalesce(sum(balance) filter (where idx <= 10), 0),
        coalesce(sum(balance) filter (where idx > 10 and idx <= 100), 0),
        coalesce(sum(balance) filter (where idx > 100 and idx <= 1000), 0),
        coalesce(sum(balance) filter (where idx > 1000 and idx <= 10000), 0),
        count(*) filter (where balance >= 10000000::bigint * 100000000),
        count(*) filter (where balance >= 1000000::bigint * 100000000 and balance < 10000000::bigint * 100000000),
        count(*) filter (where balance >= 100000::bigint * 100000000 and balance < 1000000::bigint * 100000000),
        count(*) filter (where balance >= 10000::bigint * 100000000 and balance < 100000::bigint * 100000000),
        count(*) filter (where balance >= 1000::bigint * 100000000 and balance < 10000::bigint * 100000000),
        count(*) filter (where balance >= 100000000 and balance < 1000::bigint * 100000000)
    from r
""")


@app.get("/addresses/balances/summary", tags=["Hoosat addresses"])
async def get_balances_summary():
    """
    Small summary for the explorer's Addresses page: how many addresses there are (for the page count) and the
    numbers behind its two pie charts (top-holder totals in sompi, address counts per balance size).
    Chart numbers come from the daily snapshot, exactly as the page computed them from the full file before.
    """
    async with async_session() as session:
        total = (await session.execute(select(func.count()).select_from(Balance))).scalar_one()
        row = (await session.execute(_SUMMARY_SQL)).one()
        return {
            "total_addresses": total,
            "holding_sompi": {"top10": int(row[0]), "top100": int(row[1]),
                              "top1000": int(row[2]), "top10000": int(row[3])},
            "ranges": {"gt_10m": row[4], "gt_1m": row[5], "gt_100k": row[6],
                       "gt_10k": row[7], "gt_1k": row[8], "lt_1k": row[9]},
        }


@app.get("/addresses/balances/csv/yesterday", response_class=StreamingResponse, tags=["Hoosat addresses"])
async def get_balances_csv_yesterday():
    """
    Get yesterday's balances of addresses in CSV format, for 24h-change calculations.
    Populated by a daily snapshot cron job (see ~/snapshot_balances_yesterday.sh).
    """
    async with async_session() as session:
        try:
            result = await session.execute(select(BalanceYesterday).order_by(desc(BalanceYesterday.balance)))
            balances = result.scalars().all()
            return StreamingResponse(
                csv_generator(balances),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=balances-yesterday.csv"}
            )
        except Exception as e:
            logging.exception("Error generating yesterday's CSV:")
            return StreamingResponse(
                iter(["Address,Balance\n"]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=balances-yesterday.csv"}
            )


def tags_csv_generator(tags):
    try:
        for tag in tags:
            yield f"{tag.address},{tag.label}\n"
    except GeneratorExit:
        logging.info("Client disconnected during tags CSV streaming.")

@app.get("/addresses/tags/csv", response_class=StreamingResponse, tags=["Hoosat addresses"])
async def get_address_tags_csv():
    """
    Get curated address labels (e.g. "Burn Address") in CSV format, no header row.
    Backed by the address_tags table -- add new labels there, no redeploy needed.
    """
    async with async_session() as session:
        try:
            result = await session.execute(select(AddressTag))
            tags = result.scalars().all()
            return StreamingResponse(
                tags_csv_generator(tags),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=tags.csv"}
            )
        except Exception as e:
            logging.exception("Error generating tags CSV:")
            return StreamingResponse(iter([]), media_type="text/csv")
