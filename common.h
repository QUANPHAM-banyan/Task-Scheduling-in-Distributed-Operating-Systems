#ifndef COMMON_H
#define COMMON_H

#define MAX_WORKERS 32
#define MAX_TASKS 1024
#define BUFFER_SIZE 1024

#define HEARTBEAT_INTERVAL 2
#define HEARTBEAT_TIMEOUT 6

typedef enum {
    WORKER_ALIVE,
    WORKER_FAILED
} WorkerStatus;

typedef enum {
    TASK_READY,
    TASK_RUNNING,
    TASK_COMPLETED
} TaskStatus;

typedef enum {
    POLICY_FIFO,
    POLICY_ROUND_ROBIN,
    POLICY_LEAST_LOADED
} SchedulePolicy;

typedef struct {
    int worker_id;
    int socket_fd;
    int cpu_cores;
    int current_load;
    WorkerStatus status;
    long last_heartbeat;
} WorkerInfo;

typedef struct {
    int task_id;
    char operation[32];
    int input;
    char output[256];
    TaskStatus status;
    int assigned_worker;
} Task;

#endif