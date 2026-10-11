#!/bin/bash
# Multi-Agent Smart Factory Launcher Script
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

export PYTHONPATH="$DIR"

echo "================================================================="
echo "   🏭 Multi-Agent Smart Factory System — Quick Setup & Launch   "
echo "================================================================="

if [ ! -d "$DIR/venv" ]; then
    echo "[1/3] Creating Python virtual environment..."
    python3 -m venv "$DIR/venv"
fi

echo "[2/3] Activating environment and installing dependencies..."
source "$DIR/venv/bin/activate"
pip install -q -r "$DIR/requirements.txt"

echo "[3/3] Running automated verification test suite..."
pytest -q "$DIR/test_system.py"

echo ""
echo "================================================================="
echo " ✔  ALL AUTOMATED TESTS PASSED!"
echo " 🌐 Launching FastAPI Agent Server & Live Web Dashboard..."
echo " 📍 Open your browser at: http://localhost:8000"
echo "================================================================="
echo ""

python "$DIR/app.py"
