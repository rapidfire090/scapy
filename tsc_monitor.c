#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <sched.h>
#include <getopt.h>
#include <stdlib.h>
#include <x86intrin.h>

void pin_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    if (sched_setaffinity(0, sizeof(cpuset), &cpuset) != 0) {
        perror("sched_setaffinity");
    }
}

void print_usage(const char *prog) {
    printf("Usage: %s [--core N] [--freq GHz] [--threshold us] [--sleep us]\n", prog);
    printf("  --core       CPU core to pin to (default: 0)\n");
    printf("  --freq       TSC frequency in GHz (default: 2.7)\n");
    printf("  --threshold  Alert threshold in microseconds (default: 500)\n");
    printf("  --sleep      Sleep time between measurements in microseconds (default: 100000)\n");
}

int main(int argc, char *argv[]) {
    int core = 0;
    double freq_ghz = 2.7;
    uint64_t threshold_us = 500;
    uint64_t sleep_us = 100000;

    static struct option long_options[] = {
        {"core", required_argument, 0, 'c'},
        {"freq", required_argument, 0, 'f'},
        {"threshold", required_argument, 0, 't'},
        {"sleep", required_argument, 0, 's'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "c:f:t:s:", long_options, NULL)) != -1) {
        switch (opt) {
            case 'c': core = atoi(optarg); break;
            case 'f': freq_ghz = atof(optarg); break;
            case 't': threshold_us = strtoull(optarg, NULL, 10); break;
            case 's': sleep_us = strtoull(optarg, NULL, 10); break;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    pin_to_core(core);

    double cycles_per_us = freq_ghz * 1000.0;
    uint64_t threshold_cycles = (uint64_t)(threshold_us * cycles_per_us);

    printf("Monitoring TSC deltas...\n");
    printf("  Core       : %d\n", core);
    printf("  TSC freq   : %.3f GHz\n", freq_ghz);
    printf("  Threshold  : %lu us (%lu cycles)\n", threshold_us, threshold_cycles);
    printf("  Sleep time : %lu us\n\n", sleep_us);

    while (1) {
        uint64_t t1 = __rdtsc();
        usleep(sleep_us);
        uint64_t t2 = __rdtsc();

        uint64_t delta = t2 - t1;
        if (delta > threshold_cycles) {
            printf("[ALERT] High TSC delta: %lu cycles (%.1f us)\n",
                   delta, delta / cycles_per_us);
        } else {
            printf("TSC delta: %lu cycles\n", delta);
        }
    }

    return 0;
}
