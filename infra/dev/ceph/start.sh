#!/usr/bin/env bash
# Bootstrap a volume-backed development cluster, then supervise its four daemons.
# Initialization steps replay on restart; inconsistent identity fails closed.
set -euo pipefail
umask 077

: "${DEV_S3_ACCESS_KEY_ID:?}" "${DEV_S3_SECRET_ACCESS_KEY:?}" "${DEV_S3_BUCKET:?}"
[[ "$DEV_S3_BUCKET" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]] || {
  echo 'DEV_S3_BUCKET must be a valid lowercase S3 bucket name' >&2
  exit 1
}

data=/var/lib/ceph/dev
mkdir -p "$data" /run/ceph
if [[ ! -f "$data/fsid" ]]; then
  cat /proc/sys/kernel/random/uuid > "$data/fsid"
fi
fsid=$(cat "$data/fsid")
[[ "$fsid" =~ ^[0-9a-f-]{36}$ ]] || { echo 'Invalid persisted Ceph FSID' >&2; exit 1; }
cat /opt/vigilo-ceph/ceph.conf > "$data/ceph.conf"
printf '\n[global]\nfsid = %s\n' "$fsid" >> "$data/ceph.conf"
export CEPH_CONF="$data/ceph.conf"

pids=()
cleanup() {
  trap - EXIT INT TERM
  if (( ${#pids[@]} )); then
    kill -TERM "${pids[@]}" 2>/dev/null || true
    for ((attempt=0; attempt<15; attempt++)); do
      alive=false
      for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then alive=true; fi
      done
      if ! "$alive"; then break; fi
      sleep 1
    done
    kill -KILL "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# One shared startup deadline bounds retries across all readiness phases.
deadline=$((SECONDS + 180))
until_ready() {
  until "$@"; do
    for pid in "${pids[@]}"; do
      kill -0 "$pid" 2>/dev/null || { echo 'Ceph daemon exited during startup' >&2; return 1; }
    done
    (( SECONDS < deadline )) || { echo 'Ceph startup deadline exceeded' >&2; return 1; }
    sleep 1
  done
}

if [[ ! -f "$data/admin.keyring" ]]; then
  ceph-authtool --create-keyring "$data/admin.keyring" --gen-key -n client.admin \
    --cap mon 'allow *' --cap osd 'allow *' --cap mgr 'allow *' --cap mds 'allow *'
fi
if [[ ! -d "$data/mon/store.db" ]]; then
  ceph-authtool --create-keyring "$data/mon.keyring" --gen-key -n mon. --cap mon 'allow *'
  ceph-authtool "$data/mon.keyring" --import-keyring "$data/admin.keyring"
  monmaptool --create --fsid "$fsid" --add a 127.0.0.1 "$data/monmap"
  timeout 30 ceph-mon --mkfs -i a --monmap "$data/monmap" --keyring "$data/mon.keyring"
fi
ceph-mon -f -i a &
pids+=("$!")
until_ready timeout 5 ceph status

mkdir -p "$data/osd" "$data/mgr"
if [[ ! -f "$data/osd.uuid" ]]; then
  cat /proc/sys/kernel/random/uuid > "$data/osd.uuid"
fi
osd_id=$(timeout 15 ceph osd new "$(cat "$data/osd.uuid")")
[[ "$osd_id" == 0 ]] || { echo 'Unexpected development OSD identity' >&2; exit 1; }
timeout 15 ceph auth get-or-create osd.0 mon 'allow profile osd' mgr 'allow profile osd' \
  osd 'allow *' -o "$data/osd/keyring"
if [[ ! -f "$data/osd/whoami" ]]; then
  timeout 60 ceph-osd --mkfs -i 0 --osd-uuid "$(cat "$data/osd.uuid")" \
    --keyring "$data/osd/keyring"
fi
ceph-osd -f -i 0 --keyring "$data/osd/keyring" &
pids+=("$!")

timeout 15 ceph auth get-or-create mgr.a mon 'allow profile mgr' osd 'allow *' mds 'allow *' \
  -o "$data/mgr/keyring"
ceph-mgr -f -i a --keyring "$data/mgr/keyring" &
pids+=("$!")
timeout 15 ceph auth get-or-create client.rgw.dev mon 'allow rw' osd 'allow rwx' \
  -o "$data/rgw.keyring"

# Wait for the OSD to serve writes before running the gateway's bootstrap I/O.
until_ready timeout 5 ceph osd pool create vigilo-dev-readiness 8
until_ready timeout 5 rados -p vigilo-dev-readiness put readiness /dev/null

timeout 15 radosgw-admin user list --max-entries 100 > /run/ceph/users.json
user_exists=$(python3 - <<'PYCODE'
import json
with open('/run/ceph/users.json') as stream:
    users = json.load(stream)
if isinstance(users, dict):
    assert not users.get('truncated', False), 'Development user inventory exceeded its bound'
    users = users['keys']
assert isinstance(users, list), 'Invalid development user inventory'
print('true' if 'vigilo-dev' in users else 'false')
PYCODE
)
if "$user_exists"; then
  timeout 15 radosgw-admin user info --uid vigilo-dev > /run/ceph/user.json
else
  timeout 15 radosgw-admin user create --uid vigilo-dev --display-name 'Vigilo development' \
    --access-key "$DEV_S3_ACCESS_KEY_ID" --secret-key "$DEV_S3_SECRET_ACCESS_KEY" > /run/ceph/user.json
fi
python3 - <<'PYCODE'
import json, os
with open('/run/ceph/user.json') as stream:
    keys = json.load(stream)['keys']
assert any(k['access_key'] == os.environ['DEV_S3_ACCESS_KEY_ID'] and
           k['secret_key'] == os.environ['DEV_S3_SECRET_ACCESS_KEY'] for k in keys), \
    'Configured S3 credentials differ from the persisted development user'
PYCODE
radosgw -f -n client.rgw.dev --keyring "$data/rgw.keyring" &
pids+=("$!")
until_ready python3.12 /opt/vigilo-ceph/bucket.py create
echo 'Ceph S3 development bucket is ready'

# Any daemon exit tears down the others; Compose can restart the whole cluster.
wait -n "${pids[@]}" || true
echo 'Ceph daemon exited unexpectedly' >&2
exit 1
