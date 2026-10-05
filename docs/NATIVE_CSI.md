<!-- Copyright (c) 2026 ZyvorAI Labs Private Limited. -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# atlas-native CSI driver

`crates/atlas-native-csi` (bin `atlas-native-csi`, image `Dockerfile.native-csi`) is a Container
Storage Interface driver for atlas-native filesystems ([`NATIVE_FS.md`](NATIVE_FS.md)). Driver
name: `native.atlas.zyvor.ai`.

| Kubernetes object | atlas-native object |
|---|---|
| PersistentVolume (from a PVC) | filesystem (`POST /v1/fs`) |
| VolumeSnapshot | filesystem snapshot (`POST /v1/fs/{fs}/snapshots`) |
| PVC with a VolumeSnapshot `dataSource` | clone of the snapshot (`POST /v1/fs-snapshots/{id}/clone`) |
| PVC with a PVC `dataSource` | clone of a transient snapshot of the source, deleted once the clone exists |
| Pod mount | one `atlas-native-mount` FUSE process on the pod's volume path |

Every access mode works (ReadWriteOnce, ReadOnlyMany, ReadWriteMany, ReadWriteOncePod): each
pod's mount is its own client of a shared filesystem, and cross-mount consistency is the
filesystem's (`NATIVE_FS.md`, "Consistency"). Not supported: block volumes, volume expansion,
capacity limits (filesystems are thin and unquotaed; the PVC's requested size is reported back
unchanged and nothing enforces it), topology, and volume stats.

## Install

The `atlas-native` Helm chart deploys it next to the storage nodes:

```
helm upgrade --install native deploy/helm/atlas-native \
  --set csi.enabled=true \
  --set csi.snapshots.enabled=true       # needs the VolumeSnapshot CRDs and snapshot-controller
```

This adds the `CSIDriver` object (`attachRequired: false`, `fsGroupPolicy: File`), a controller
Deployment (atlas-native-csi with the external-provisioner and, with snapshots enabled, the
external-snapshotter sidecar, leader-elected), a node DaemonSet (atlas-native-csi plus the
node-driver-registrar), the StorageClass `atlas-native-fs`, and the VolumeSnapshotClass
`atlas-native-fs`. Both plugins reach the storage nodes through their per-pod DNS names with the
release's API token Secret; with `httpTls.enabled` they trust the Secret's `ca.crt` (only that key
is mounted). `httpTls.requireClientCert` is not supported with the CSI driver yet.

The node plugin runs privileged with `/dev/fuse` and the kubelet directory mounted
`Bidirectional`, so the FUSE mounts it makes are visible to the kubelet and the pods.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: checkpoints }
spec:
  accessModes: [ReadWriteMany]
  storageClassName: atlas-native-fs
  resources: { requests: { storage: 100Gi } }
```

## StorageClass options

| Field | Effect |
|---|---|
| `parameters.extentBytes` (`csi.storageClass.extentBytes`) | Extent grid of new filesystems in bytes (default: the cluster's `extent_bytes`) |
| `mountOptions` (`csi.storageClass.mountOptions`) | `atlas-native-mount` flags: `ro`, `cache-leases`, `direct-reads`, and `<flag>=<n>` for `ttl-ms`, `writeback-bytes`, `writeback-parallel`, `readahead-bytes`, `max-io-bytes`, `fuse-threads`, `session-ttl-ms`, `retry-secs` |

Any other parameter or mount option is refused (InvalidArgument), so a StorageClass can't point a
mount at another endpoint or credential.

## Behaviour

- **Idempotent creates.** The volume id is the CSI name when it is a valid atlas-native id of at
  most 48 characters (the provisioner's `pvc-<uuid>` and `snapshot-<uuid>`), else `csi-` plus 40
  hex digits of its SHA-256. A retried CreateVolume or CreateSnapshot lands on the object the
  first attempt made; the same snapshot name on a different source volume is AlreadyExists.
- **Deletes** of a missing volume or snapshot succeed. Clones keep their own references to the
  shared extents, so deleting a source volume or snapshot never affects its clones.
- **Publish** spawns `atlas-native-mount --fs <id> --allow-other [--read-only] <target>` (the
  connection flags pass file paths only, never the token itself) and waits up to
  `--mount-timeout-secs` (30) for the target to appear in `/proc/self/mountinfo`. Publishing a
  healthy mounted target again is a no-op; a target whose FUSE process is gone (`ENOTCONN`) is
  unmounted and mounted again.
- **Unpublish** unmounts with `umount2(2)` (falling back to a lazy unmount), reaps the mount
  process, and removes the target directory. It does not use `fusermount3 -u`: Ubuntu's AppArmor
  profile for `fusermount3` refuses unmounts under the kubelet root even from a privileged
  container (found on the lab).
- **Restarting a node plugin pod breaks the mounts it made.** The FUSE processes are its
  children, so a DaemonSet rollout or plugin crash leaves the pods on that node with
  `Transport endpoint is not connected` until they are restarted; the replacement plugin then
  unmounts the dead mounts and publishes fresh ones. The chart's node DaemonSet therefore uses
  `updateStrategy: OnDelete` (`csi.node.updateStrategy`): a chart upgrade doesn't roll it; drain
  a node, then delete its plugin pod to pick up a new image.

## Verification

`cargo test -p atlas-native-csi`: the controller over a real Unix socket against an in-process
cluster (idempotent create, hashed names, block refused, snapshot create retry with a stable
creation time, AlreadyExists across volumes, snapshot and volume clones checked through the FUSE
operations layer, NotFound sources, validate, idempotent deletes, clones outliving their sources),
the node service's validation, mount-option whitelisting, and mountinfo parsing.

Live on the lab's single-node k3s (v1.36, Ubuntu 26.04, chart with `replicas=1`, local-path
state PVC), 2026-10-05:

- The node plugin registered with the kubelet; a ReadWriteMany PVC bound and two pods with
  different uids mounted it at once (`fuse` mount, `allow_other`, `default_permissions`). An 8 MiB
  random file written by one pod read back with the same MD5 in the other, and `fsGroup` ownership
  was applied.
- A VolumeSnapshot became `readyToUse`; a PVC restored from it held the files from before the
  snapshot and not a file written after it, while a PVC cloned from the live PVC held both, with
  matching checksums.
- After deleting the node plugin pod, the existing mounts returned `ENOTCONN` (as described
  above). The first build's unpublish then failed on the `fusermount3` AppArmor denial; with the
  `umount2` fix, kubelet's retry unpublished the dead mount and a recreated pod remounted the
  volume.
- Deleting the pods left no FUSE mounts or mount processes on the host; deleting the
  VolumeSnapshot and PVCs left `GET /v1/fs` and `GET /v1/fs-snapshots` empty.

Not yet verified: multi-node clusters (pods on different nodes sharing an RWX volume), HTTPS
endpoints, and throughput through the CSI mount (it is the same `atlas-native-mount` measured in
`NATIVE_FS.md`).
