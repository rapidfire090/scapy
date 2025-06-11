import ctypes
import mmap
import struct
import time

# Allocate executable memory and write machine code for rdtsc
def make_rdtsc():
    # x86_64 machine code: rdtsc; shl rdx, 32; or rax, rdx; ret
    code = b'\x0f\x31\x48\xc1\xe2\x20\x48\x09\xd0\xc3'

    # Allocate RWX memory
    buf = mmap.mmap(-1, len(code), prot=mmap.PROT_READ | mmap.PROT_WRITE | mmap.PROT_EXEC)
    buf.write(code)

    # Create a function pointer to the machine code
    address = ctypes.addressof(ctypes.c_char.from_buffer(buf))
    rdtsc_func = ctypes.CFUNCTYPE(ctypes.c_uint64)(address)

    return rdtsc_func

# Setup
rdtsc = make_rdtsc()

# Estimate TSC frequency (or use known value)
TSC_FREQ_GHZ = 2.7  # Adjust for your system
CYCLES_PER_US = TSC_FREQ_GHZ * 1000
THRESHOLD_US = 500
THRESHOLD_CYCLES = int(CYCLES_PER_US * THRESHOLD_US)
SLEEP_TIME = 0.1  # seconds

print(f"Monitoring TSC deltas... Threshold: {THRESHOLD_US} µs ({THRESHOLD_CYCLES:.0f} cycles)")

# Loop
while True:
    t1 = rdtsc()
    time.sleep(SLEEP_TIME)
    t2 = rdtsc()
    delta = t2 - t1

    if delta > THRESHOLD_CYCLES:
        print(f"[ALERT] High TSC delta: {delta} cycles (~{delta / CYCLES_PER_US:.1f} µs)")
    else:
        print(f"TSC delta: {delta} cycles")
