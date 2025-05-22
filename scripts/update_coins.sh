#!/usr/bin/env bash
COINS=(bitcoin ethereum cardano dogecoin litecoin the-open-network)
for id in "${COINS[@]}"; do
  echo "Fetching $id..."
  curl -s "https://api.coingecko.com/api/v3/coins/$id/market_chart?vs_currency=usd&days=365" \
    -o "${id}_raw.json"
  jq '{prices: .prices}' "${id}_raw.json" > "${id}_365d.json"
  rm "${id}_raw.json"
  git add "${id}_365d.json"
done

git commit -m "Add 365-day price series for 6 cryptos"
git push origin main
