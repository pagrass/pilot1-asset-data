
## Updating price series

Whenever you want fresh 180-day data for our six cryptos, simply run:

```bash
cd pilot1-asset-data
./scripts/update_coins.sh

git add README.md
git commit -m "Document how to update price series in README"
git push origin main
cat << 'EOF' > Makefile
.PHONY: update-coins

update-coins:
\t./scripts/update_coins.sh

## Updating price series

Whenever you want fresh 180-day data for our six cryptos, simply run:

