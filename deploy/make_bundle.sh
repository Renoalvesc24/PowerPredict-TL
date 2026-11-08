#!/usr/bin/env bash
# Runs on the development PC (Git Bash). Builds deploy/powerpredict_bundle.tar.gz:
#   project code (no docs, no .env, no big data) + the active Random Forest model + a trimmed database seed.
#   bash deploy/make_bundle.sh [number_of_latest_live_readings]
set -euo pipefail
cd "$(dirname "$0")/.."
ROWS="${1:-20000}"
DUMP="/c/xampp/mysql/bin/mysqldump.exe"
DB=powerpredict_db
SEED=deploy/db_seed.sql
NODATA="tbl_readings_live tbl_user_sessions tbl_heartbeats tbl_stream_offsets"

echo "== database seed (latest $ROWS live readings; the server adds new ones live)"
ign=""; for t in $NODATA; do ign="$ign --ignore-table=$DB.$t"; done
"$DUMP" -u root --single-transaction --default-character-set=utf8mb4 $ign "$DB" > "$SEED"
"$DUMP" -u root --single-transaction --default-character-set=utf8mb4 --no-data "$DB" $NODATA >> "$SEED"
"$DUMP" -u root --single-transaction --default-character-set=utf8mb4 --no-create-info \
  --where="id > (SELECT m FROM (SELECT MAX(id) - $ROWS AS m FROM tbl_readings_live) x)" "$DB" tbl_readings_live >> "$SEED"
gzip -f -9 "$SEED"
ls -la "$SEED.gz"

echo "== bundle"
model=$(python -c "
import sys; sys.path.insert(0, '.')
from ml.predict import active_model_row
r = active_model_row('supervised'); print(r['file_path'].replace(chr(92), '/'))")
git ls-files --cached --others --exclude-standard | grep -vE '^(docs/|\.claude/|tests/)' > deploy/.bundle_list
printf '%s\n' "$model" deploy/db_seed.sql.gz >> deploy/.bundle_list
tar -czf deploy/powerpredict_bundle.tar.gz -T deploy/.bundle_list
rm deploy/.bundle_list
ls -la deploy/powerpredict_bundle.tar.gz
