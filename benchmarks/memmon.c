/* Separate memory pass: sample engine processes in this container, excluding self.
 * RSS/HWM can count shared pages multiple times. PSS apportions shared pages.
 * HWM is lifetime per-process, and its sum need not be a simultaneous peak. */
#define _POSIX_C_SOURCE 200809L
#include <dirent.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static volatile sig_atomic_t running = 1;
static void stop(int signal) { (void)signal; running = 0; }
static long long value(const char *path) {
    FILE *f = fopen(path, "r"); long long n = -1;
    if (f) { if (fscanf(f, "%lld", &n) != 1) n = -1; fclose(f); }
    return n;
}
int main(int argc, char **argv) {
    if (argc != 2) return 2;
    FILE *out = fopen(argv[1], "w");
    if (!out) { perror("open output"); return 2; }
    signal(SIGTERM, stop); signal(SIGINT, stop);
    const struct timespec delay = {0, 50000000};
    while (running) {
        long long rss = 0, pss = 0, private = 0, hwm = 0;
        int processes = 0, smaps = 0;
        DIR *dir = opendir("/proc"); struct dirent *entry;
        if (!dir) return 2;
        while ((entry = readdir(dir))) {
            char *end; long pid = strtol(entry->d_name, &end, 10);
            if (*end || pid <= 0 || pid == getpid()) continue;
            char path[128], line[512];
            snprintf(path, sizeof(path), "/proc/%ld/status", pid);
            FILE *f = fopen(path, "r");
            if (!f) continue;
            int resident_status = 0;
            while (fgets(line, sizeof(line), f)) {
                long long n;
                if (sscanf(line, "VmRSS: %lld kB", &n) == 1) { rss += n * 1024; resident_status = 1; }
                if (sscanf(line, "VmHWM: %lld kB", &n) == 1) hwm += n * 1024;
            }
            fclose(f);
            /* Zombies have no resident address space or smaps. Do not mistake
             * an exiting process for a PSS permission failure. */
            if (!resident_status) continue;
            processes++;
            snprintf(path, sizeof(path), "/proc/%ld/smaps_rollup", pid);
            f = fopen(path, "r");
            if (!f) continue;
            smaps++;
            while (fgets(line, sizeof(line), f)) {
                long long n;
                if (sscanf(line, "Pss: %lld kB", &n) == 1) pss += n * 1024;
                if (sscanf(line, "Private_Clean: %lld kB", &n) == 1) private += n * 1024;
                if (sscanf(line, "Private_Dirty: %lld kB", &n) == 1) private += n * 1024;
            }
            fclose(f);
        }
        closedir(dir);
        struct timespec now; clock_gettime(CLOCK_REALTIME, &now);
        fprintf(out, "{\"time\":%.6f,\"rss_bytes\":%lld,\"pss_bytes\":%lld,"
            "\"private_bytes\":%lld,\"sum_process_hwm_bytes\":%lld,\"processes\":%d,"
            "\"smaps_readable\":%d,\"cgroup_current_bytes\":%lld,\"cgroup_peak_bytes\":%lld}\n",
            now.tv_sec + now.tv_nsec / 1e9, rss, pss, private, hwm, processes, smaps,
            value("/sys/fs/cgroup/memory.current"), value("/sys/fs/cgroup/memory.peak"));
        fflush(out);
        nanosleep(&delay, NULL);
    }
    fclose(out); return 0;
}
