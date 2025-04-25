#!/usr/bin/env bash
COINS=(bitcoin ethereum cardano vechain solana toncoin)
for id in "${COINS[@]}"; do
  echo "Fetching $id..."
  curl -s "https://api.coingecko.com/api/v3/coins/$id/market_chart?vs_currency=usd&days=180" \
    -o "${id}_raw.json"
  jq '{prices: .prices}' "${id}_raw.json" > "${id}_180d.json"
  rm "${id}_raw.json"
  git add "${id}_180d.json"
done

git commit -m "Add 180-day price series for 6 cryptos"
git push origin main
