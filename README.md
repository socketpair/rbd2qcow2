# rbd2qcow2
Backup program which INCREMENTALLY back up CEPH RBD images to a chain of qcow2 files

First of all, requirements:
* Python >= 3.5
* Python RBD and Python Rados compiled (using Cython for Python 3). These libraries you
  may get from official Ceph git repository. Please note that Kraken distro is broken
  (from syntax errors to undefined externals). I use patched kraken version, I will
  include these .pyx files and setup.py for their compilation later.
* qemu-nbd and qemu-img. I advice to use qemu-nbd which supports discarding and zero-detection.

This script is used in production
