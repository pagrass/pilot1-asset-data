#./scripts/update_coins.sh
COINS=(bitcoin ethereum cardano cosmos quant)

for id in "${COINS[@]}"; do
  echo "Fetching $id..."
  curl -s "https://api.coingecko.com/api/v3/coins/$id/market_chart?vs_currency=usd&days=721" \
    -o "${id}_raw.json"
  
  if jq -e '.prices' "${id}_raw.json" > /dev/null; then
    jq '{prices: .prices}' "${id}_raw.json" > "${id}_721d.json"
    rm "${id}_raw.json"
    git add "${id}_721d.json"
  else
    echo "⚠️ Failed to fetch data for $id. Possibly rate-limited or invalid response."
    cat "${id}_raw.json"
    rm "${id}_raw.json"
  fi
  
  sleep 15  # Add delay to avoid rate limit
done

git commit -m "Add 721-day price series for 6 cryptos"
git push origin main
