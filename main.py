#!/usr/bin/env python3
"""
Trade Executor Service - WITH AUTHENTICATION
"""
import os
import asyncio
import json
import logging
import functools
from datetime import datetime, timezone
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger("TradeExecutorService")

app = Flask(__name__)
trade_queue = []
executed_trades = []

# API KEY AUTH
API_KEY = os.environ.get("API_KEY", "echoforge-trade-2026")

def require_auth(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get('Authorization', '')
        key = request.headers.get('X-API-Key', '')
        if auth == f"Bearer {API_KEY}" or key == API_KEY:
            return f(*args, **kwargs)
        return jsonify({"error": "Unauthorized"}), 401
    return decorated

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "service": "trade-executor-v3", "timestamp": datetime.now(timezone.utc).isoformat()})

@app.route('/signal', methods=['POST'])
@require_auth
def receive_signal():
    data = request.json
    log.info(f"Signal received: {json.dumps(data)}")
    trade_queue.append(data)
    return jsonify({"status": "queued", "signal": data})

@app.route('/execute', methods=['POST'])
@require_auth
def execute_trade():
    data = request.json
    log.info(f"Execute request: {json.dumps(data)}")
    # Simulation for now - real executor needs PRIVATE_KEY
    result = {"status": "simulated", "action": data.get("action"), "chain": data.get("chain"), "amount": data.get("amount")}
    executed_trades.append(result)
    return jsonify(result)

@app.route('/queue', methods=['GET'])
@require_auth
def get_queue():
    return jsonify({"pending": trade_queue})

@app.route('/history', methods=['GET'])
@require_auth
def get_history():
    return jsonify({"executed": executed_trades[-50:]})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    log.info(f"Trade Executor Service (AUTH ENABLED) starting on port {port}")
    app.run(host="0.0.0.0", port=port)
