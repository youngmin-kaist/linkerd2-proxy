/* armclk.c - aarch64 core clock without perf/PMU/root (BF-3 boots ACPI: no
 * device-tree clock-frequency; perf_event_paranoid=4 blocks user PMU). A
 * dependency chain of `add` has ALU latency 1 on Cortex-A78, so retired adds
 * == cycles. Validated vs perf on jet1: 2.127 vs 2.133 GHz (0.3%). Build:
 * gcc -O2 -o armclk armclk.c ; run pinned on an idle core: taskset -c 15 ./armclk
 * Bind registers through operands (as here) - hardcoding x0/x1 via
 * register-asm mis-binds and the loop never terminates. (from jet-bf-dmesh) */
#include <stdio.h>
#include <stdint.h>
#include <time.h>
#define A4  "add %0, %0, #1\n\t" "add %0, %0, #1\n\t" "add %0, %0, #1\n\t" "add %0, %0, #1\n\t"
#define A16 A4 A4 A4 A4
static double measure(uint64_t iters)
{
    struct timespec a, b; uint64_t x = 0, n = iters;
    clock_gettime(CLOCK_MONOTONIC, &a);
    __asm__ volatile("1:\n\t" A16 "subs %1, %1, #1\n\t" "b.ne 1b\n\t" : "+r"(x), "+r"(n) : : "cc");
    clock_gettime(CLOCK_MONOTONIC, &b);
    double sec = (b.tv_sec - a.tv_sec) + (b.tv_nsec - a.tv_nsec) * 1e-9;
    return (double)iters * 16.0 / sec / 1e9;
}
int main(void){ measure(1000000); for (int i = 0; i < 3; i++) printf("  %.3f GHz\n", measure(20000000ULL)); return 0; }
