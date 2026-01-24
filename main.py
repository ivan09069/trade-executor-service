#!/usr/bin/env python3
"""
Trade Executor Service - HTTP API wrapper for SwarmSentinel Trade Executor v3
"""
import os
import asyncio
import json
import logging
from datetime import datetime, timezone
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger("TradeExecutorService")

app = Flask(__name__)
trade_queue = []
executed_trades = []

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "service": "trade-executor-v3", "timestamp": datetime.now(timezone.utc).isoformat()})

@app.route('/signal', methods=['POST'])
def receive_signal():
    data = request.json
    log.info(f"Signal received: {json.dumps(data)}")
    trade_queue.append(data)
    return jsonify({"status": "queued", "signal": data})

@app.route('/execute', methods=['POST'])
def execute_trade():
    from executor import TradeExecutor, Action, Chain
    data = request.json
    executor = TradeExecutor()
    action = Action(data["action"].lower())
    chain = Chain(data["chain"].lower())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    decision = loop.run_until_complete(executor.reasoning.decide(action, chain, data["token_in"].upper(), data["token_out"].upper(), float(data["amount"]), float(data.get("max_risk", 0.5))))
    result = {"decision": {"action": decision.action.value, "chain": decision.chain.value, "risk": decision.risk, "execute": decision.execute}}
    if decision.execute:
        exec_result = loop.run_until_complete(executor.execute(decision, live=data.get("live", False)))
        result["execution"] = exec_result
    loop.close()
    return jsonify(result)

@app.route('/queue', methods=['GET'])
def get_queue():
    return jsonify({"pending": trade_queue})

@app.route('/history', methods=['GET'])
def get_history():
    return jsonify({"executed": executed_trades[-50:]})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    log.info(f"Trade Executor Service starting on port {port}")
    app.run(host="0.0.0.0", port=port)
