#!/usr/bin/env python3
"""Test whether the current recorder environment allows creating an io_uring."""

import ctypes
import os
import platform
import sys

if platform.system() != "Linux" or platform.machine() not in ("x86_64", "aarch64"):
    sys.exit("this probe supports Linux x86_64 and aarch64")
libc = ctypes.CDLL(None, use_errno=True)
# Zero-initialized struct io_uring_params from the Linux UAPI (120 bytes).
params = (ctypes.c_uint64 * 15)()
descriptor = libc.syscall(ctypes.c_long(425), ctypes.c_uint(2), ctypes.byref(params))
if descriptor < 0:
    error = ctypes.get_errno()
    sys.exit(f"io_uring_setup: errno={error} ({os.strerror(error)})")
os.close(descriptor)
print("io_uring_setup succeeded")
