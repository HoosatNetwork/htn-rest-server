#!/bin/bash
# Daily snapshot of the balances table into balances_yesterday, used by the
# explorer's 24h-change column and the /addresses/balances/summary endpoint.
#
# Run once a day via cron on the host running the pg16 container, e.g.:
#   0 0 * * * /path/to/scripts/snapshot_balances_yesterday.sh >> /var/log/snapshot_balances_yesterday.log 2>&1

docker exec pg16 psql -U htn_user -d htn_database -c "
TRUNCATE balances_yesterday;
INSERT INTO balances_yesterday (script_public_key_address, balance)
SELECT script_public_key_address, balance FROM balances;
"
