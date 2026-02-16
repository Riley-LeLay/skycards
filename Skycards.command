#!/bin/bash
cd "$(dirname "$0")"

echo ""
echo "==================================="
echo "   Skycards Rare Plane Finder"
echo "==================================="
echo ""
echo "Ultra rare planes (rarity 10+) are always shown on the map."
echo ""
echo "Enter your daily challenges below (up to 3)."
echo "Type each challenge and press Enter, or just press Enter to skip."
echo ""

CHALLENGES=()

for i in 1 2 3; do
    printf "  Challenge %d: " "$i"
    read -r line
    if [ -z "$line" ]; then
        break
    fi
    CHALLENGES+=("$line")
done

# Build the command
CMD=(python3 main.py --serve)
for ch in "${CHALLENGES[@]}"; do
    CMD+=(--challenge "$ch")
done

echo ""
if [ ${#CHALLENGES[@]} -gt 0 ]; then
    echo "Starting with ${#CHALLENGES[@]} challenge(s) + ultra rare planes..."
else
    echo "Starting with ultra rare planes only..."
fi
echo ""

"${CMD[@]}"
