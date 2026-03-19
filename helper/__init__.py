# encoding: utf-8
import asyncio
import logging
import time

import aiocache
import aiohttp
from aiocache import cached

FLOOD_DETECTED = False
CACHE = None

_logger = logging.getLogger(__name__)

aiocache.logger.setLevel(logging.WARNING)


@cached(ttl=120)
async def get_htn_price():
    market_data = await get_htn_market_data()
    if market_data is None:
        raise ValueError("Market data could not be retrieved")
    return market_data.get("current_price", {}).get("usd", "Price unavailable")


@cached(ttl=300)
async def get_htn_market_data():
    global FLOOD_DETECTED
    global CACHE
    if not FLOOD_DETECTED or time.time() - FLOOD_DETECTED > 300:
        # Try coinpaprika as fallback
        _logger.debug("Querying CoinPaprika now.")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.coinpaprika.com/v1/tickers/htn-hoosat-network", timeout=10) as resp:
                    if resp.status == 200:
                        FLOOD_DETECTED = False
                        data = await resp.json()
                        # Transform coinpaprika data to match coingecko format
                        usd_data = data["quotes"]["USD"]
                        CACHE = {
                            "current_price": {
                                "usd": usd_data.get("price", 0)
                            },
                            "market_cap": {
                                "usd": usd_data.get("market_cap", 0)
                            },
                            "total_volume": {
                                "usd": usd_data.get("volume_24h", 0)
                            },
                            "price_change_percentage_24h": usd_data.get("percent_change_24h", 0),
                            "price_change_percentage_7d": usd_data.get("percent_change_7d", 0),
                            "price_change_percentage_30d": usd_data.get("percent_change_30d", 0),
                            "price_change_percentage_1y": usd_data.get("percent_change_1y", 0),
                            "ath": {
                                "usd": usd_data.get("ath_price", 0)
                            },
                            "ath_change_percentage": {
                                "usd": usd_data.get("percent_from_price_ath", 0)
                            }
                        }
                        return CACHE
                    elif resp.status == 429:
                        FLOOD_DETECTED = time.time()
                        if CACHE:
                            _logger.warning('Using cached value. 429 detected.')
                        _logger.warning("Rate limit exceeded.")
                    else:
                        _logger.error(f"CoinPaprika failed with status code {resp.status}")
        except asyncio.TimeoutError:
            _logger.error("Timeout occurred while querying CoinPaprika")
        except Exception as e:
            _logger.error(f"Error querying CoinPaprika: {e}")
        
        # Try coingecko first
        _logger.debug("Querying CoinGecko now.")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.coingecko.com/api/v3/coins/hoosat-network", timeout=10) as resp:
                    if resp.status == 200:
                        FLOOD_DETECTED = False
                        CACHE = (await resp.json())["market_data"]
                        return CACHE
                    elif resp.status == 429:
                        FLOOD_DETECTED = time.time()
                        if CACHE:
                            _logger.warning('Using cached value. 429 detected.')
                        _logger.warning("Rate limit exceeded.")
                    else:
                        _logger.error(f"CoinGecko failed with status code {resp.status}")
        except asyncio.TimeoutError:
            _logger.error("Timeout occurred while querying CoinGecko")
        except Exception as e:
            _logger.error(f"Error querying CoinGecko: {e}")
        
        # Try nonkyc as final fallback
        _logger.debug("Querying NonKYC now.")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.nonkyc.io/api/v2/ticker/HTN%2FUSDT", timeout=10) as resp:
                    if resp.status == 200:
                        FLOOD_DETECTED = False
                        data = (await resp.json())
                        _logger.info(data)
                        CACHE = { 'current_price': {'usd': float(data['last_price'])} }
                        return CACHE
                    elif resp.status == 429:
                        FLOOD_DETECTED = time.time()
                        if CACHE:
                            _logger.warning('Using cached value. 429 detected.')
                        _logger.warning("Rate limit exceeded.")
                    else:
                        _logger.error(f"NonKYC failed with status code {resp.status}")
        except asyncio.TimeoutError:
            _logger.error("Timeout occurred while querying NonKYC")
        except Exception as e:
            _logger.error(f"Error querying NonKYC: {e}")

    return CACHE
