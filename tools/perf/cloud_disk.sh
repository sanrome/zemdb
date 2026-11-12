#!/usr/bin/env bash
# An emulated cloud disk, with a limit of write operations per second, for E2-disco (Lote 7 of
# Fase 3.12). A file with ext4 inside, as a loop device with direct I/O; a second loop device over
# the first, mounted on MNT. The write IOPS limit of blkio (cgroup v1) goes on the lower device,
# where each write and each flush that the upper one sends arrives as one bio: so the limit counts
# the requests of the filesystem after their merges, as a disk does, flushes included. On the
# filesystem's own device, it would count bios before their merges (about twice as many with these
# loads). With cgroup v2, the same with io.max. Root only; a reboot of the machine loses the devices.
#   cloud_disk.sh setup IMG MNT [SIZE]   creates IMG (3G by default) if missing, and mounts it on MNT
#   cloud_disk.sh check IMG MNT          fails unless IMG is mounted on MNT as above
#   cloud_disk.sh limit IMG IOPS         sets the limit (0 removes it)
#   cloud_disk.sh disk IMG               the name of the upper device, for --disk of the generator
set -euo pipefail
THROTTLE=/sys/fs/cgroup/blkio/blkio.throttle.write_iops_device
lower() { losetup -nO NAME -j "$1" | head -1; }
upper() { local low; low=$(lower "$1"); [ -n "$low" ] && losetup -nO NAME -j "$low" | head -1; }
mounted() { local up; up=$(upper "$1") && [ -n "$up" ] && [ "$(findmnt -no SOURCE "$2")" = "$up" ]; }
case $1 in
  setup)
    IMG=$2; MNT=$3
    mounted "$IMG" "$MNT" && exit 0
    if [ ! -e "$IMG" ]; then
      truncate -s "${4:-3G}" "$IMG"
      mkfs.ext4 -q -F "$IMG"
    fi
    low=$(lower "$IMG")
    [ -n "$low" ] || low=$(losetup -f --show --direct-io=on "$IMG")
    up=$(losetup -nO NAME -j "$low" | head -1)
    [ -n "$up" ] || up=$(losetup -f --show --direct-io=on "$low")
    mkdir -p "$MNT"
    mount "$up" "$MNT"
    mounted "$IMG" "$MNT" ;;
  check) mounted "$2" "$3" || { echo "the emulated disk $2 is not mounted on $3" >&2; exit 1; } ;;
  limit)
    low=$(lower "$2"); [ -n "$low" ] || { echo "no loop device over $2" >&2; exit 1; }
    echo "$(cat "/sys/block/${low#/dev/}/dev") $3" > "$THROTTLE" ;;
  disk) up=$(upper "$2"); echo "${up#/dev/}" ;;
  *) echo "unknown command $1" >&2; exit 1 ;;
esac
