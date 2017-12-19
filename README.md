# rbd2qcow2
Backup program which INCREMENTALLY back up CEPH RBD images to a chain of qcow2 files

First of all, requirements:
* Python >= 3.5
* Python3 RBD and Python3 Rados binding.
* qemu-nbd and qemu-img. I advice to use qemu-nbd which supports discarding and zero-detection.

This tool basically will look for all RBD images (by default) in specified CEPH pool.
For each image, it makes RBD snapshot. This snapshot is pulled basically in a same way as
`rbd export-diff --from-snap` does. And this diff is transformed to qcow2-image, forming chain of
qcow2 images. Each qcow2 image stores diff from previous one.

The main features of such way of storing snapshot diffs:

1. Ability to start VM from any qcow2 file in the chain. Since each qcow2-file in our case is
   the snapshot and also is the diff. Just like commits in GIT. Note, you MUST NOT modify
   any qcow2 file in a chain. If you do that, all backups in the chain after modified file will be
   broken. If you want to start VM immediatelly -- either create new qcow2-image which refers to chosen
   qcow2-image, or start `qemu --snapshot`.

2. Usable backups even if whole CEPH cluster is broken down. `rbd import-diff` requires
   working CEPH cluster.

3. qcow2 format is standard. Any qcow2 image may be easily put back to CEPH (or another storage) using
   `qemu-img` command without any vendor-specific tool.

4. Ability to remove old incremental backups. This may be implemented using
   `qcow2 rebase` operation in qemu-img.files in order to remove old backups.

5. Ability to pull diff using multiple parallel data streams. This is MUCH
   faster, since different chunks are pulled from different OSDs in parallel.

6. This tool effectively transfers regions filled with zeroes. Both
   when pulling from RBD and when writing to qcow2. In order to effectively
   store zeroes in RBD you should enable DISCARD in qemu. Configure discard
   alignment in qemu correct to match RBD chunk size (i.e. 4MB by default)
   and use `virtio-scsi` or `ide` interface (and not just `virtio`) for disks.
   Also, you need to set up `fstrim -v -all` by cron (or equivalent) in your VM.
   Do not add `discard` option while mounting FS. This is inefficient actually.
   Recent Windows versions already discard zeroes by default. If everything
   done right, zeroes (i.e. trash typically from removed files) will not be
   transferred over the network.

   And also you need to have recent qemu-nbd which supports discard operation.
   If not, this tool will just write zeroes directly.

   Anyway, if you forget something from this notes, everything will work, but not
   so efficient.

7. I pay attention to atomic disk operations. So, interrupted backups will never
   corrupt the chain.

Note:

1. This script does not backup VM metadata (i.e. hardware description).
2. This script does not make atomic snapshot of VM with two or more disks.
3. qcow2 compressing is still not used (will be used in future). Unfortunatelly
   it can not be used while writing image. It's only possible using
   qemu-img convert. But I don't know how to compress only diff, not resulting
   image. It seems This will be done when I rewrite RBD layer from scratch and
   eliminate qemu-nbd.
4. This tool uses qemu-nbd to write qcow2 images. Unfortunatelly, there is no
   library for such task. Only image format specification.
5. Do not remove old backups. Since qcow2 images form a chain, removing any part
   from the chain will break it. If you need to remove old backups, use
   `qemu-img rebase`. In future, it will be implemented in this tool.
6. Do not copy qcow2 files. They contain only diff, and so they can not be used
   without all previous images. Either copy image and all previous files, or
   use qemu-img convert to copy from some backed up qcow2 file to target one.
   I recommend to enable compression im `qqmu-img convert` if target file
   supports compression.
7. Do not run this tool simultaneously on same set of images, especially with
   same target directory. It will possibly corrupt logic.

8. This script is used in production ;)
9. Patches, suggestions, questions, feature requests are welcome.
10. This tool should work on CEPH versions Jewel and upper.

