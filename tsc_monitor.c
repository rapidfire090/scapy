#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <sched.h>

// Default values
#define DEFAULT_SLEEP_US 10000
#define DEFAULT_THRESHOLD_US 500
#define DEFAULT_FREQ_GHZ 2.7

static inline uint64_t rdtsc(void) {
    unsigned int lo, hi;
    __asm__ volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    return ((uint64_t)hi << 32) | lo;
}

void print_usage(char *prog) {
    fprintf(stderr, "Usage: %s [-f freq_ghz] [-t threshold_us] [-s sleep_us] [-c core] [-v]\n", prog);
    fprintf(stderr, "  -f CPU frequency in GHz (default %.1f)\n", DEFAULT_FREQ_GHZ);
    fprintf(stderr, "  -t Threshold in microseconds (default %d us)\n", DEFAULT_THRESHOLD_US);
    fprintf(stderr, "  -s Sleep interval in microseconds (default %d us)\n", DEFAULT_SLEEP_US);
    fprintf(stderr, "  -c Pin to core (optional)\n");
    fprintf(stderr, "  -v Verbose mode: print every delta (default: only print when above threshold)\n");
}

int main(int argc, char **argv) {
    double freq_ghz = DEFAULT_FREQ_GHZ;
    int threshold_us = DEFAULT_THRESHOLD_US;
    int sleep_us = DEFAULT_SLEEP_US;
    int core = -1;
    int verbose = 0;
    int opt;

    while ((opt = getopt(argc, argv, "f:t:s:c:v")) != -1) {
        switch (opt) {
            case 'f': freq_ghz = atof(optarg); break;
            case 't': threshold_us = atoi(optarg); break;
            case 's': sleep_us = atoi(optarg); break;
            case 'c': core = atoi(optarg); break;
            case 'v': verbose = 1; break;
            default: print_usage(argv[0]); return 1;
        }
    }

    if (core >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core, &cpuset);
        if (sched_setaffinity(0, sizeof(cpuset), &cpuset) != 0) {
            perror("sched_setaffinity");
            return 1;
        }
    }

    uint64_t threshold_cycles = (uint64_t)(threshold_us * freq_ghz * 1000.0);
    struct timespec sleep_time = { .tv_sec = sleep_us / 1000000, .tv_nsec = (sleep_us % 1000000) * 1000 };

    while (1) {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);  // timestamp before t1
        uint64_t t1 = rdtsc();
        nanosleep(&sleep_time, NULL);
        uint64_t t2 = rdtsc();

        uint64_t delta = t2 - t1;

        if (verbose || delta > threshold_cycles) {
            printf("[%ld.%09ld] delta: %lu cycles (%.2f us)\n",
                   now.tv_sec, now.tv_nsec,
                   delta,
                   delta / (freq_ghz * 1000.0));
            fflush(stdout);
        }
    }

    return 0;
}
