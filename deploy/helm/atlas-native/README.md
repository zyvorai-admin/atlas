<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# atlas-native Helm chart

Runs `atlas-native-node` ([`docs/NATIVE_NODE.md`](../../../docs/NATIVE_NODE.md)) as a StatefulSet:
every pod is a Raft metadata voter and a data node serving its own PVC. Node ids are pod names,
peers are addressed by the headless Service's stable DNS names, and one generated config is
shared by all pods (`${POD_NAME}` is expanded at startup).

```sh
helm upgrade --install atlas-native deploy/helm/atlas-native -n atlas-native --create-namespace \
  --set image.repository=ghcr.io/zyvorai/atlas-native-node --set image.tag=0.4.0
kubectl -n atlas-native get secret atlas-native-api -o jsonpath='{.data.token}' | base64 -d
```

| Value | Default | Notes |
| --- | --- | --- |
| `replicas` | 3 | Voters = data nodes. Odd numbers; not resizable by `helm upgrade` alone. |
| `node.replicationFactor` | 3 | Copies per extent, at most `replicas`. |
| `node.extentBytes`, `node.maxRequestBytes`, `node.tickMs`, `node.proposalTimeoutMs`, `node.repairIntervalSecs`, `node.gcIntervalSecs` | see `values.yaml` | Map to the node config fields of the same name. |
| `apiToken.existingSecret` | empty | Secret with key `token`; otherwise the chart creates `<fullname>-api` with a random token kept across upgrades (and on uninstall). |
| `tls.enabled` | false | Mutual TLS for Raft and data-node traffic. The certificate must carry every pod name as a DNS SAN. |
| `tls.existingSecret` / `tls.certManager.*` | empty / off | `ca.crt`, `tls.crt`, `tls.key`; or let cert-manager issue `<fullname>-tls` from `issuerRef` with the right SANs. |
| `httpTls.enabled`, `httpTls.existingSecret` | off | HTTPS for the API (`tls.crt`, `tls.key`); probes switch to HTTPS. |
| `httpTls.requireClientCert` | false | Require a client certificate signed by the Secret's `ca.crt` on `/v1/*`; probes and `/metrics` stay open. |
| `persistence.size`, `persistence.storageClass` | 5Gi, default class | Per-pod state volume. |
| `podAntiAffinity` | soft | `soft`, `hard` or `none`. |
| `serviceMonitor.enabled` | false | Prometheus Operator `ServiceMonitor` for `/metrics`. |

Pods roll only when the rendered config or the pod template changes (`checksum/config`), so an
unchanged `helm upgrade` is a no-op. `deploy/native/helm-live-check.sh` installs the chart with
both TLS layers on a throwaway PKI and checks auth, I/O, leader failover and that no-op upgrade.
