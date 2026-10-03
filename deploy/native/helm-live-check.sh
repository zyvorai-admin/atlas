#!/usr/bin/env bash
# Copyright (c) 2026 ZyvorAI Labs Private Limited.
# SPDX-License-Identifier: Apache-2.0
#
# Installs deploy/helm/atlas-native on the current kube context with Raft/data mutual TLS and
# HTTPS + client-certificate auth (throwaway openssl PKI), then checks: auth refusals, unaligned
# I/O through the leader, leader-pod failover, and that an unchanged `helm upgrade` restarts no
# pods. Run from the repo root on a host with kubectl, helm, openssl and curl.
#
#   deploy/native/helm-live-check.sh <image-repository> <image-tag> [namespace]
set -euo pipefail

REPO=${1:?image repository}
TAG=${2:?image tag}
NS=${3:-atlas-native-helm}
REL=atlas-native
PKI=$(mktemp -d)
PF_PIDS=()
cleanup() {
  for p in "${PF_PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
  rm -rf "$PKI"
}
trap cleanup EXIT

leaf() { # name sans eku
  openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=$1" \
    -keyout "$PKI/$1.key" -out "$PKI/$1.csr" 2>/dev/null
  printf 'subjectAltName=%s\nextendedKeyUsage=%s\nbasicConstraints=CA:FALSE\n' "$2" "$3" >"$PKI/$1.ext"
  openssl x509 -req -in "$PKI/$1.csr" -CA "$PKI/ca.crt" -CAkey "$PKI/ca.key" -CAcreateserial \
    -days 2 -extfile "$PKI/$1.ext" -out "$PKI/$1.crt" 2>/dev/null
}
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 -subj "/CN=atlas-native-test-ca" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout "$PKI/ca.key" -out "$PKI/ca.crt" 2>/dev/null
leaf raft "DNS:$REL-0,DNS:$REL-1,DNS:$REL-2" "serverAuth,clientAuth"
leaf http "DNS:localhost,DNS:$REL-api.$NS.svc" "serverAuth"
leaf client "DNS:ops" "clientAuth"

kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
for s in raft http; do
  kubectl -n "$NS" create secret generic "$REL-$s-tls" --from-file=ca.crt="$PKI/ca.crt" \
    --from-file=tls.crt="$PKI/$s.crt" --from-file=tls.key="$PKI/$s.key" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done

install() {
  helm upgrade --install "$REL" deploy/helm/atlas-native -n "$NS" --wait --timeout 5m \
    --set image.repository="$REPO",image.tag="$TAG",image.pullPolicy=Never \
    --set tls.enabled=true,tls.existingSecret="$REL-raft-tls" \
    --set httpTls.enabled=true,httpTls.existingSecret="$REL-http-tls",httpTls.requireClientCert=true \
    --set persistence.size=1Gi >/dev/null
}
install
kubectl -n "$NS" rollout status statefulset/$REL --timeout=300s
echo "helm install: ok"

TOKEN=$(kubectl -n "$NS" get secret "$REL-api" -o jsonpath='{.data.token}' | base64 -d)
forward() { # pod local-port
  kubectl -n "$NS" port-forward "pod/$1" "$2:7480" >/dev/null 2>&1 &
  PF_PIDS+=($!)
  for _ in $(seq 1 50); do
    curl -sf --cacert "$PKI/ca.crt" "https://localhost:$2/healthz" >/dev/null && return 0
    sleep 0.2
  done
  echo "port-forward to $1 failed" >&2
  return 1
}
url() { echo "https://localhost:$1$2"; }
call() { # port path [curl args...]
  local port=$1 path=$2
  shift 2
  curl -sS --cacert "$PKI/ca.crt" --cert "$PKI/client.crt" --key "$PKI/client.key" \
    -H "Authorization: Bearer $TOKEN" "$@" "$(url "$port" "$path")"
}
leader_of() { call "$1" /v1/status | sed -nE 's/.*"leader":"([^"]+)".*/\1/p'; }

forward "$REL-0" 18480
code=$(curl -s -o /dev/null -w '%{http_code}' --cacert "$PKI/ca.crt" -H "Authorization: Bearer $TOKEN" "$(url 18480 /v1/status)")
[ "$code" = 401 ] || { echo "no client cert: want 401, got $code" >&2; exit 1; }
echo "no client certificate -> 401: ok"
code=$(curl -s -o /dev/null -w '%{http_code}' --cacert "$PKI/ca.crt" "$(url 18480 /readyz)")
[ "$code" = 200 ] || { echo "readyz without cert: $code" >&2; exit 1; }
echo "readyz without client certificate -> 200: ok"

LEADER=""
for _ in $(seq 1 100); do LEADER=$(leader_of 18480); [ -n "$LEADER" ] && break; sleep 0.2; done
[ -n "$LEADER" ] || { echo "no leader" >&2; exit 1; }
echo "leader: $LEADER (raft over mTLS)"
forward "$LEADER" 18481
VOL=$(call 18481 /v1/volumes -X POST -d '{"name":"helm-check","size_bytes":16777216}' |
  sed -nE 's/.*"id":"([^"]+)".*/\1/p')
[ -n "$VOL" ] || { echo "volume create failed" >&2; exit 1; }
head -c 100000 /dev/urandom >"$PKI/block"
call 18481 "/v1/volumes/$VOL/data?offset=4194000" -X PUT --data-binary @"$PKI/block"
call 18481 "/v1/volumes/$VOL/data?offset=4194000&len=100000" -o "$PKI/back"
cmp "$PKI/block" "$PKI/back"
echo "unaligned write/read across an extent boundary via $LEADER: ok"

kubectl -n "$NS" delete pod "$LEADER" --wait=false >/dev/null
OTHER=$(for i in 0 1 2; do [ "$REL-$i" = "$LEADER" ] || { echo "$REL-$i"; break; }; done)
forward "$OTHER" 18482
NEW=""
for _ in $(seq 1 150); do
  NEW=$(leader_of 18482 || true)
  [ -n "$NEW" ] && [ "$NEW" != "$LEADER" ] && break
  sleep 0.2
done
[ -n "$NEW" ] && [ "$NEW" != "$LEADER" ] || { echo "no new leader" >&2; exit 1; }
forward "$NEW" 18483
call 18483 "/v1/volumes/$VOL/data?offset=4194000&len=100000" -o "$PKI/back2"
cmp "$PKI/block" "$PKI/back2"
echo "failover $LEADER -> $NEW, read back: ok"
kubectl -n "$NS" rollout status statefulset/$REL --timeout=300s >/dev/null

before=$(kubectl -n "$NS" get pods -l app.kubernetes.io/instance=$REL -o jsonpath='{range .items[*]}{.metadata.uid} {end}')
install
after=$(kubectl -n "$NS" get pods -l app.kubernetes.io/instance=$REL -o jsonpath='{range .items[*]}{.metadata.uid} {end}')
[ "$before" = "$after" ] || { echo "unchanged upgrade restarted pods" >&2; exit 1; }
echo "unchanged helm upgrade kept every pod: ok"
echo "helm live check: ok"
