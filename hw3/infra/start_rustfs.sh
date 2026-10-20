#!/bin/bash
cd "$(dirname "$0")/../work"
mkdir -p disks/disk{1,2,3,4}
docker rm -f rustfs 2>/dev/null
MSYS_NO_PATHCONV=1 docker run -d --name rustfs -p 9000:9000 -p 9001:9001 \
  -e RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin \
  -e "RUSTFS_VOLUMES=/data/disk{1...4}" \
  -v "$(pwd -W)/disks/disk1:/data/disk1" -v "$(pwd -W)/disks/disk2:/data/disk2" \
  -v "$(pwd -W)/disks/disk3:/data/disk3" -v "$(pwd -W)/disks/disk4:/data/disk4" \
  rustfs/rustfs:latest
