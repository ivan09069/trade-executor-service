#!/usr/bin/env python3
"""
SWARMSENTINEL TRADE EXECUTOR v3
Live execution with web3 + real data feeds
"""

import os
import sys
import json
import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Optional, Tuple
from enum import Enum
from decimal import Decimal

# Setup logging
os.makedirs(os.path.expanduser("~/logs"), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(os.path.expanduser("~/logs/trades.log")),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("SwarmSentinel")

# Web3 imports
try:
    from web3 import Web3
    from web3.middleware import geth_poa_middleware
    from eth_account import Account
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False
    log.warning("web3 not installed - run: pip install web3")


class Chain(Enum):
    BASE = "base"
    ETH = "ethereum"
    ARB = "arbitrum"
    POLY = "polygon"

class Action(Enum):
    BUY = "buy"
    SELL = "sell"
    SWAP = "swap"


@dataclass
class Step:
    num: int
    action: str
    reasoning: str
    confidence: float
    data: Dict = field(default_factory=dict)
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def __str__(self):
        return f"[{self.num}] {self.action}: {self.reasoning} ({self.confidence:.0%})"


@dataclass
class Decision:
    action: Action
    chain: Chain
    token_in: str
    token_out: str
    amount: float
    steps: List[Step]
    risk: float
    confidence: float
    execute: bool
    gas_estimate: float = 0.0
    price_impact: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# Token addresses per chain
TOKENS = {
    Chain.BASE: {
        "ETH": "0x4200000000000000000000000000000000000006",
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
        "DAI": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
        "AERO": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
        "BRETT": "0x532f27101965dd16442E59d40670FaF5eBB142E4",
    },
    Chain.ETH: {
        "ETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "DAI": "0x6B175474E89094C44Da98b954EessdcdFD72257",
    },
    Chain.ARB: {
        "ETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
    },
    Chain.POLY: {
        "MATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "WMATIC": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "USDC": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
    },
}

# Chain configs
CHAINS = {
    Chain.BASE: {
        "rpc": os.getenv("BASE_RPC", "https://mainnet.base.org"),
        "chain_id": 8453,
        "router": "0x327Df1E6de05895d2ab08513aaDD9313Fe505d86",  # BaseSwap
        "router_v3": "0x2626664c2603336E57B271c5C0b26F421741e481",  # Uniswap V3
        "dexscreener": "base",
        "gas_token": "ETH",
        "avg_gas": 0.00005,
        "poa": False,
    },
    Chain.ETH: {
        "rpc": os.getenv("ETH_RPC", "https://eth.llamarpc.com"),
        "chain_id": 1,
        "router": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",  # Uniswap V2
        "router_v3": "0xE592427A0AEce92De3Edee1F18E0157C05861564",  # Uniswap V3
        "dexscreener": "ethereum",
        "gas_token": "ETH",
        "avg_gas": 0.005,
        "poa": False,
    },
    Chain.ARB: {
        "rpc": os.getenv("ARB_RPC", "https://arb1.arbitrum.io/rpc"),
        "chain_id": 42161,
        "router": "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",  # SushiSwap
        "router_v3": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
        "dexscreener": "arbitrum",
        "gas_token": "ETH",
        "avg_gas": 0.0001,
        "poa": False,
    },
    Chain.POLY: {
        "rpc": os.getenv("POLY_RPC", "https://polygon-rpc.com"),
        "chain_id": 137,
        "router": "0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff",  # QuickSwap
        "dexscreener": "polygon",
        "gas_token": "MATIC",
        "avg_gas": 0.01,
        "poa": True,
    },
}

# Router ABI (Uniswap V2 style)
ROUTER_ABI = [
    {
        "inputs": [
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"}
        ],
        "name": "swapExactETHForTokens",
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
        "stateMutability": "payable",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"}
        ],
        "name": "swapExactTokensForETH",
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"}
        ],
        "name": "swapExactTokensForTokens",
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "path", "type": "address[]"}
        ],
        "name": "getAmountsOut",
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function"
    },
]

ERC20_ABI = [
    {"inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "name": "approve", "outputs": [{"name": "", "type": "bool"}], "stateMutability": "nonpayable", "type": "function"},
    {"inputs": [{"name": "account", "type": "address"}],
     "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
     "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "stateMutability": "view", "type": "function"},
]


class DataFeed:
    """Real-time market data"""
    
    DEXSCREENER = "https://api.dexscreener.com/latest/dex"
    COINGECKO = "https://api.coingecko.com/api/v3"
    
    def __init__(self):
        self.cache: Dict[str, Tuple[Dict, float]] = {}
        self.cache_ttl = 30
    
    async def get_token_data(self, token: str, chain: Chain, session: aiohttp.ClientSession) -> Dict:
        key = f"{chain.value}:{token}"
        now = datetime.now(timezone.utc).timestamp()
        
        if key in self.cache:
            data, ts = self.cache[key]
            if now - ts < self.cache_ttl:
                return data
        
        data = await self._fetch_dexscreener(token, chain, session)
        if not data.get("price"):
            data = await self._fetch_coingecko(token, session)
        
        self.cache[key] = (data, now)
        return data
    
    async def _fetch_dexscreener(self, token: str, chain: Chain, session: aiohttp.ClientSession) -> Dict:
        try:
            chain_name = CHAINS[chain]["dexscreener"]
            token_addr = TOKENS.get(chain, {}).get(token, token)
            
            url = f"{self.DEXSCREENER}/tokens/{token_addr}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.json()
                    pairs = [p for p in data.get("pairs", []) if p.get("chainId") == chain_name]
                    
                    if pairs:
                        best = max(pairs, key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0))
                        return {
                            "price": float(best.get("priceUsd", 0) or 0),
                            "liquidity": float(best.get("liquidity", {}).get("usd", 0) or 0),
                            "volume_24h": float(best.get("volume", {}).get("h24", 0) or 0),
                            "price_change_24h": float(best.get("priceChange", {}).get("h24", 0) or 0),
                            "pair": best.get("pairAddress"),
                            "dex": best.get("dexId"),
                            "source": "dexscreener",
                        }
        except Exception as e:
            log.debug(f"DexScreener error: {e}")
        return {}
    
    async def _fetch_coingecko(self, token: str, session: aiohttp.ClientSession) -> Dict:
        token_map = {"ETH": "ethereum", "WETH": "weth", "USDC": "usd-coin", "USDT": "tether", "DAI": "dai", "ARB": "arbitrum", "MATIC": "matic-network"}
        cg_id = token_map.get(token.upper())
        if not cg_id:
            return {}
        
        try:
            url = f"{self.COINGECKO}/simple/price?ids={cg_id}&vs_currencies=usd&include_24hr_change=true"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.json()
                    info = data.get(cg_id, {})
                    return {"price": info.get("usd", 0), "price_change_24h": info.get("usd_24h_change", 0), "liquidity": 1e9, "source": "coingecko"}
        except:
            pass
        return {}


class ReasoningEngine:
    """Chain-of-thought reasoning"""
    
    def __init__(self):
        self.steps: List[Step] = []
        self.n = 0
        self.feed = DataFeed()
    
    def think(self, action: str, reasoning: str, confidence: float, data: Dict = None) -> Step:
        self.n += 1
        step = Step(self.n, action, reasoning, confidence, data or {})
        self.steps.append(step)
        log.info(str(step))
        return step
    
    def reset(self):
        self.steps = []
        self.n = 0
    
    async def analyze_token(self, token: str, chain: Chain, session: aiohttp.ClientSession) -> Dict:
        self.think("ANALYZE", f"Fetching {token} on {chain.value}", 0.9)
        data = await self.feed.get_token_data(token, chain, session)
        
        if data.get("price"):
            self.think("DATA", f"${data['price']:.6f} | Liq ${data.get('liquidity',0):,.0f} | {data.get('source','')}", 0.85, data)
        else:
            self.think("DATA", "⚠️ No data - using defaults", 0.5)
            data = {"price": 0, "liquidity": 100000, "volume_24h": 50000, "price_change_24h": 0}
        return data
    
    def calc_liquidity_risk(self, amount_usd: float, liquidity: float) -> Tuple[bool, float]:
        if liquidity == 0:
            self.think("LIQUIDITY", "❌ ZERO liquidity", 0.3)
            return False, 0.5
        ratio = liquidity / max(amount_usd, 1)
        if ratio < 5:
            self.think("LIQUIDITY", f"❌ {ratio:.1f}x - too low", 0.4)
            return False, 0.45
        elif ratio < 10:
            self.think("LIQUIDITY", f"⚠️ {ratio:.1f}x - low", 0.6)
            return True, 0.3
        elif ratio < 50:
            self.think("LIQUIDITY", f"⚡ {ratio:.1f}x - medium", 0.8)
            return True, 0.15
        self.think("LIQUIDITY", f"✅ {ratio:.1f}x - high", 0.95)
        return True, 0.03
    
    def calc_volatility_risk(self, pct: float) -> float:
        pct = abs(pct)
        if pct > 50:
            self.think("VOLATILITY", f"🔥 {pct:.1f}% - extreme", 0.3)
            return 0.45
        elif pct > 25:
            self.think("VOLATILITY", f"⚠️ {pct:.1f}% - high", 0.5)
            return 0.3
        elif pct > 10:
            self.think("VOLATILITY", f"⚡ {pct:.1f}% - elevated", 0.7)
            return 0.15
        self.think("VOLATILITY", f"✅ {pct:.1f}% - normal", 0.9)
        return 0.03
    
    def calc_slippage(self, amount_usd: float, liquidity: float) -> float:
        if liquidity == 0:
            return 0.5
        impact = min(amount_usd / (2 * liquidity) * 100, 50)
        level = "❌" if impact > 5 else "⚠️" if impact > 1 else "✅"
        self.think("SLIPPAGE", f"{level} {impact:.2f}% impact", 0.9 if impact < 1 else 0.6)
        return impact / 100
    
    async def calculate_risk(self, token_data: Dict, amount: float, price: float) -> Tuple[float, float]:
        self.think("RISK_CALC", "Computing risk...", 0.9)
        amount_usd = amount * (price if price > 0 else 3000)
        liquidity = token_data.get("liquidity", 0)
        
        _, liq_risk = self.calc_liquidity_risk(amount_usd, liquidity)
        vol_risk = self.calc_volatility_risk(token_data.get("price_change_24h", 0))
        slip_risk = self.calc_slippage(amount_usd, liquidity)
        
        total = liq_risk * 0.35 + vol_risk * 0.25 + slip_risk * 0.25 + 0.15 * min(amount_usd / max(token_data.get("volume_24h", 1), 1) * 0.1, 0.3)
        total = min(total + max(liq_risk, vol_risk, slip_risk) * 0.2, 1.0)
        
        self.think("RISK_SCORE", f"Final: {total:.1%}", 0.9, {"risk": total})
        return total, slip_risk
    
    async def decide(self, action: Action, chain: Chain, token_in: str, token_out: str, amount: float, max_risk: float = 0.5) -> Decision:
        self.reset()
        self.think("INIT", f"{action.value.upper()}: {amount} {token_in} → {token_out} on {chain.value}", 0.95)
        
        async with aiohttp.ClientSession() as session:
            token_data = await self.analyze_token(token_out, chain, session)
            in_data = await self.feed.get_token_data(token_in, chain, session)
            risk, impact = await self.calculate_risk(token_data, amount, in_data.get("price", 3000))
            
            execute = risk <= max_risk
            self.think("DECISION", f"{'✅ APPROVED' if execute else '❌ REJECTED'} - Risk {risk:.1%}", 1 - risk)
            
            return Decision(action=action, chain=chain, token_in=token_in, token_out=token_out,
                           amount=amount, steps=self.steps, risk=risk, confidence=1-risk,
                           execute=execute, gas_estimate=CHAINS[chain]["avg_gas"], price_impact=impact)


class TradeExecutor:
    """Multi-chain trade execution with web3"""
    
    def __init__(self, private_key: str = None):
        self.pk = private_key or os.getenv("PRIVATE_KEY")
        self.reasoning = ReasoningEngine()
        self.history: List[Dict] = []
        self.web3_instances: Dict[Chain, Web3] = {}
        self._load_history()
        
        if WEB3_AVAILABLE and self.pk:
            self._init_web3()
    
    def _init_web3(self):
        """Initialize web3 connections per chain"""
        for chain, config in CHAINS.items():
            try:
                w3 = Web3(Web3.HTTPProvider(config["rpc"]))
                if config.get("poa"):
                    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
                if w3.is_connected():
                    self.web3_instances[chain] = w3
                    log.info(f"Connected to {chain.value}: {config['rpc']}")
            except Exception as e:
                log.warning(f"Failed to connect to {chain.value}: {e}")
    
    def _load_history(self):
        path = os.path.expanduser("~/trading/history/trades.json")
        if os.path.exists(path):
            try:
                with open(path) as f:
                    self.history = json.load(f)
            except:
                self.history = []
    
    def _save_history(self):
        path = os.path.expanduser("~/trading/history/trades.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            json.dump(self.history[-1000:], f, indent=2, default=str)
    
    def get_wallet_address(self) -> Optional[str]:
        if not self.pk or not WEB3_AVAILABLE:
            return None
        return Account.from_key(self.pk).address
    
    async def get_balance(self, chain: Chain, token: str = "ETH") -> float:
        """Get token balance"""
        w3 = self.web3_instances.get(chain)
        if not w3:
            return 0.0
        
        addr = self.get_wallet_address()
        if not addr:
            return 0.0
        
        try:
            if token.upper() in ["ETH", "MATIC", "WETH", "WMATIC"]:
                bal = w3.eth.get_balance(addr)
                return float(w3.from_wei(bal, 'ether'))
            else:
                token_addr = TOKENS.get(chain, {}).get(token.upper())
                if token_addr:
                    contract = w3.eth.contract(address=Web3.to_checksum_address(token_addr), abi=ERC20_ABI)
                    decimals = contract.functions.decimals().call()
                    bal = contract.functions.balanceOf(addr).call()
                    return bal / (10 ** decimals)
        except Exception as e:
            log.error(f"Balance check failed: {e}")
        return 0.0
    
    async def approve_token(self, chain: Chain, token: str, amount: float, spender: str) -> Optional[str]:
        """Approve token spending"""
        w3 = self.web3_instances.get(chain)
        if not w3 or not self.pk:
            return None
        
        token_addr = TOKENS.get(chain, {}).get(token.upper())
        if not token_addr:
            return None
        
        try:
            contract = w3.eth.contract(address=Web3.to_checksum_address(token_addr), abi=ERC20_ABI)
            decimals = contract.functions.decimals().call()
            amount_wei = int(amount * (10 ** decimals))
            
            # Check current allowance
            addr = self.get_wallet_address()
            allowance = contract.functions.allowance(addr, Web3.to_checksum_address(spender)).call()
            
            if allowance >= amount_wei:
                self.reasoning.think("APPROVE", f"Already approved {amount} {token}", 0.95)
                return "already_approved"
            
            # Build approval tx
            nonce = w3.eth.get_transaction_count(addr)
            gas_price = w3.eth.gas_price
            
            tx = contract.functions.approve(
                Web3.to_checksum_address(spender),
                amount_wei
            ).build_transaction({
                'from': addr,
                'nonce': nonce,
                'gasPrice': gas_price,
                'gas': 100000,
                'chainId': CHAINS[chain]["chain_id"],
            })
            
            signed = w3.eth.account.sign_transaction(tx, self.pk)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            
            self.reasoning.think("APPROVE", f"Approval tx: {tx_hash.hex()}", 0.9)
            
            # Wait for confirmation
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            if receipt.status == 1:
                self.reasoning.think("APPROVE", f"✅ Approved {amount} {token}", 0.95)
                return tx_hash.hex()
            else:
                self.reasoning.think("APPROVE", "❌ Approval failed", 0.3)
                return None
                
        except Exception as e:
            log.error(f"Approval failed: {e}")
            self.reasoning.think("APPROVE", f"❌ Error: {str(e)[:50]}", 0.3)
            return None

    
    async def execute_swap(self, decision: Decision) -> Dict:
        """Execute swap on-chain"""
        chain = decision.chain
        w3 = self.web3_instances.get(chain)
        config = CHAINS.get(chain, {})
        
        if not w3:
            return {"status": "error", "reason": f"No web3 connection for {chain.value}"}
        if not self.pk:
            return {"status": "error", "reason": "No private key"}
        
        addr = self.get_wallet_address()
        token_in = decision.token_in.upper()
        token_out = decision.token_out.upper()
        amount = decision.amount
        
        try:
            router = w3.eth.contract(
                address=Web3.to_checksum_address(config["router"]),
                abi=ROUTER_ABI
            )
            
            # Get token addresses
            token_in_addr = TOKENS.get(chain, {}).get(token_in, token_in)
            token_out_addr = TOKENS.get(chain, {}).get(token_out, token_out)
            weth = config.get("router").replace(config["router"], TOKENS.get(chain, {}).get("WETH", ""))
            
            # Determine swap type
            is_eth_in = token_in in ["ETH", "MATIC", "WETH", "WMATIC"]
            is_eth_out = token_out in ["ETH", "MATIC", "WETH", "WMATIC"]
            
            nonce = w3.eth.get_transaction_count(addr)
            gas_price = w3.eth.gas_price
            deadline = int(datetime.now(timezone.utc).timestamp()) + 300  # 5 min
            
            # Calculate amount in wei
            if is_eth_in:
                amount_wei = w3.to_wei(amount, 'ether')
            else:
                token_contract = w3.eth.contract(address=Web3.to_checksum_address(token_in_addr), abi=ERC20_ABI)
                decimals = token_contract.functions.decimals().call()
                amount_wei = int(amount * (10 ** decimals))
            
            # Get expected output
            path = [Web3.to_checksum_address(token_in_addr), Web3.to_checksum_address(token_out_addr)]
            
            try:
                amounts_out = router.functions.getAmountsOut(amount_wei, path).call()
                expected_out = amounts_out[-1]
                min_out = int(expected_out * (1 - decision.price_impact - 0.01))  # Add 1% slippage tolerance
            except:
                min_out = 0  # Will likely fail but let's try
                self.reasoning.think("QUOTE", "⚠️ Could not get quote, using 0 min", 0.5)
            
            self.reasoning.think("EXECUTE", f"Building swap tx: {amount} {token_in} → {token_out}", 0.9)
            
            # Build transaction based on type
            if is_eth_in:
                # ETH -> Token
                tx = router.functions.swapExactETHForTokens(
                    min_out,
                    path,
                    addr,
                    deadline
                ).build_transaction({
                    'from': addr,
                    'value': amount_wei,
                    'nonce': nonce,
                    'gasPrice': gas_price,
                    'gas': 300000,
                    'chainId': config["chain_id"],
                })
            elif is_eth_out:
                # Token -> ETH (need approval first)
                approval = await self.approve_token(chain, token_in, amount, config["router"])
                if not approval:
                    return {"status": "error", "reason": "Token approval failed"}
                
                tx = router.functions.swapExactTokensForETH(
                    amount_wei,
                    min_out,
                    path,
                    addr,
                    deadline
                ).build_transaction({
                    'from': addr,
                    'nonce': nonce + (1 if approval != "already_approved" else 0),
                    'gasPrice': gas_price,
                    'gas': 300000,
                    'chainId': config["chain_id"],
                })
            else:
                # Token -> Token (need approval first)
                approval = await self.approve_token(chain, token_in, amount, config["router"])
                if not approval:
                    return {"status": "error", "reason": "Token approval failed"}
                
                tx = router.functions.swapExactTokensForTokens(
                    amount_wei,
                    min_out,
                    path,
                    addr,
                    deadline
                ).build_transaction({
                    'from': addr,
                    'nonce': nonce + (1 if approval != "already_approved" else 0),
                    'gasPrice': gas_price,
                    'gas': 300000,
                    'chainId': config["chain_id"],
                })
            
            # Sign and send
            signed = w3.eth.account.sign_transaction(tx, self.pk)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            
            self.reasoning.think("BROADCAST", f"TX: {tx_hash.hex()}", 0.9)
            
            # Wait for confirmation
            self.reasoning.think("CONFIRM", "Waiting for confirmation...", 0.8)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
            
            if receipt.status == 1:
                self.reasoning.think("SUCCESS", f"✅ Confirmed in block {receipt.blockNumber}", 0.95)
                return {
                    "status": "success",
                    "tx_hash": tx_hash.hex(),
                    "block": receipt.blockNumber,
                    "gas_used": receipt.gasUsed,
                }
            else:
                self.reasoning.think("FAILED", "❌ Transaction reverted", 0.3)
                return {"status": "reverted", "tx_hash": tx_hash.hex()}
                
        except Exception as e:
            log.error(f"Swap execution failed: {e}")
            self.reasoning.think("ERROR", f"❌ {str(e)[:80]}", 0.2)
            return {"status": "error", "reason": str(e)}

    
    async def execute(self, decision: Decision, live: bool = False) -> Dict:
        """Main execution entry point"""
        
        if not decision.execute:
            return {"status": "rejected", "reason": f"Risk {decision.risk:.1%} exceeds threshold", "decision": asdict(decision)}
        
        config = CHAINS.get(decision.chain, {})
        
        result = {
            "chain": decision.chain.value,
            "chain_id": config.get("chain_id"),
            "router": config.get("router"),
            "token_in": decision.token_in,
            "token_out": decision.token_out,
            "amount": decision.amount,
            "risk": decision.risk,
            "impact": decision.price_impact,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "simulated",
        }
        
        if live:
            if not WEB3_AVAILABLE:
                return {"status": "error", "reason": "web3 not installed - run: pip install web3"}
            if not self.pk:
                return {"status": "error", "reason": "No PRIVATE_KEY set"}
            if decision.chain not in self.web3_instances:
                return {"status": "error", "reason": f"No connection to {decision.chain.value}"}
            
            self.reasoning.think("LIVE", "🔴 LIVE EXECUTION MODE", 0.9)
            
            # Check balance
            bal = await self.get_balance(decision.chain, decision.token_in)
            if bal < decision.amount:
                return {"status": "error", "reason": f"Insufficient balance: {bal} < {decision.amount} {decision.token_in}"}
            
            self.reasoning.think("BALANCE", f"✅ Balance: {bal} {decision.token_in}", 0.9)
            
            # Execute swap
            swap_result = await self.execute_swap(decision)
            result.update(swap_result)
        else:
            self.reasoning.think("SIM", "🟡 Simulated execution", 0.85)
        
        # Save to history
        self.history.append(result)
        self._save_history()
        
        log.info(f"Trade result: {json.dumps(result, default=str)}")
        return result


def print_decision(d: Decision):
    """Pretty print"""
    print("\n" + "="*60)
    print("🤖 SWARMSENTINEL v3")
    print("="*60)
    print(f"Action:     {d.action.value.upper()}")
    print(f"Chain:      {d.chain.value}")
    print(f"Swap:       {d.amount} {d.token_in} → {d.token_out}")
    print(f"Risk:       {d.risk:.1%}")
    print(f"Impact:     {d.price_impact:.2%}")
    print(f"Confidence: {d.confidence:.1%}")
    print(f"Execute:    {'✅ YES' if d.execute else '❌ NO'}")
    print("\n📋 REASONING:")
    for step in d.steps:
        print(f"  {step}")
    print("="*60)


async def main_async():
    args = sys.argv[1:]
    
    if not args or args[0] in ["-h", "--help", "help"]:
        print("""
🤖 SWARMSENTINEL EXECUTOR v3 - LIVE TRADING
============================================

Usage:
  python trade_executor_v3.py <action> <chain> <token_in> <token_out> <amount> [max_risk] [--live]

Actions: buy, sell, swap
Chains:  base, ethereum, arbitrum, polygon

Examples:
  # Simulated
  python trade_executor_v3.py buy base ETH USDC 0.1
  
  # LIVE EXECUTION (requires PRIVATE_KEY env var)
  python trade_executor_v3.py swap base ETH USDC 0.05 0.3 --live

Environment:
  PRIVATE_KEY  - Wallet private key (required for --live)
  BASE_RPC     - Base RPC URL
  ETH_RPC      - Ethereum RPC URL
  ARB_RPC      - Arbitrum RPC URL
  POLY_RPC     - Polygon RPC URL

Features:
  ✅ Real-time DexScreener + CoinGecko data
  ✅ Weighted risk scoring
  ✅ Live web3 execution
  ✅ Auto token approval
  ✅ Slippage protection
  ✅ Trade history logging
        """)
        return
    
    # Parse args
    live = "--live" in args
    args = [a for a in args if a != "--live"]
    
    action = Action(args[0].lower())
    chain = Chain(args[1].lower())
    token_in = args[2].upper()
    token_out = args[3].upper()
    amount = float(args[4])
    max_risk = float(args[5]) if len(args) > 5 else 0.5
    
    # Execute
    executor = TradeExecutor()
    
    if live:
        print("\n⚠️  LIVE MODE - Real funds will be used!")
        addr = executor.get_wallet_address()
        if addr:
            print(f"   Wallet: {addr}")
            bal = await executor.get_balance(chain, token_in)
            print(f"   Balance: {bal} {token_in}")
        else:
            print("   ❌ No wallet configured")
            return
    
    decision = await executor.reasoning.decide(action, chain, token_in, token_out, amount, max_risk)
    print_decision(decision)
    
    if decision.execute:
        if live:
            confirm = input("\n🔴 Confirm LIVE execution? (yes/no): ")
            if confirm.lower() != "yes":
                print("Cancelled.")
                return
        
        print("\n⏳ Executing...")
        result = await executor.execute(decision, live=live)
        print(f"\n📊 Result: {result.get('status', 'unknown')}")
        
        if result.get("tx_hash"):
            chain_explorer = {
                Chain.BASE: "https://basescan.org/tx/",
                Chain.ETH: "https://etherscan.io/tx/",
                Chain.ARB: "https://arbiscan.io/tx/",
                Chain.POLY: "https://polygonscan.com/tx/",
            }
            print(f"   🔗 {chain_explorer.get(chain, '')}{result['tx_hash']}")
        
        if result.get("error") or result.get("reason"):
            print(f"   ❌ {result.get('reason', result.get('error', 'Unknown error'))}")
    else:
        print(f"\n⛔ Not executed - risk {decision.risk:.1%} > max {max_risk:.1%}")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
