import time
import ctypes
import os

# Define rdtsc using ctypes and inline assembly
class RDTSC:
    def __init__(self):
        self.lib = ctypes.CDLL(None)
        self.rdtsc_func = self._build_rdtsc()

    def _build_rdtsc(self):
        # Use inline assembly with ctypes to read TSC
        # This defines a small function in memory that calls `rdtsc`
        from ctypes import CFUNCTYPE, c_uint64
        import mmap

        # x86_64 machine code for: rdtsc; shl rdx, 32; or rax, rdx; ret
        code = bytearray([
            0x0F, 0x31,                  # rdtsc
            0x48, 0xC1, 0xE2, 0x20,      # shl rdx, 32
            0x48, 0x09, 0xD0,            # or rax, rdx
            0xC3                         # ret
        ])

        # Allocate executable memory
        buf = mmap.mmap(-1, len(code), prot=mmap.PROT_READ | mmap.PROT_WRITE | mmap.PROT_EXEC)
        buf.write(code)

        # Create function pointer
        FUNC = CFUNCTYPE(c_uint64)
        return FUNC(ctypes.addressof(ctypes.c_void_p.from_buffer(buf)))

    def read(self):
        return self.rdtsc_func()

# Get base TSC frequency (or set manually if known)
TSC_FREQ_GHZ = 2.7  # adjust for your CPU (e.g. 2.7 GHz)
CYCLES_PER_US = TSC_FREQ_GHZ * 1000  # cycles per microsecond

# Threshold to flag potential SMIs (e.g., > 500µs)
THRESHOLD_US = 500
THRESHOLD_CYCLES = int(CYCLES_PER_US * THRESHOLD_US)

# Sampling interval
SLEEP_TIME = 0.1  # seconds

rdtsc = RDTSC()
print(f"Monitoring TSC deltas... Threshold: {THRESHOLD_US} µs ({THRESHOLD_CYCLES} cycles)")

while True:
    t1 = rdtsc.read()
    time.sleep(SLEEP_TIME)
    t2 = rdtsc.read()
    delta = t2 - t1

    if delta > THRESHOLD_CYCLES:
        delta_us = delta / CYCLES_PER_US
        print(f"[ALERT] High TSC delta: {delta} cycles ≈ {delta_us:.1f} µs")
    else:
        print(f"TSC delta: {delta} cycles")
