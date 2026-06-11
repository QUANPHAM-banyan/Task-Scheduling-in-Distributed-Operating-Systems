#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <time.h>

#include "common.h"

WorkerInfo workers[MAX_WORKERS];
Task tasks[MAX_TASKS];

int worker_count = 0;
int task_count = 0;
int next_worker_rr = 0;

SchedulePolicy policy = POLICY_LEAST_LOADED;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

long current_time_sec() {
    return time(NULL);
}

int find_worker_index(int worker_id) {
    for (int i = 0; i < worker_count; i++) {
        if (workers[i].worker_id == worker_id) {
            return i;
        }
    }
    return -1;
}

int find_task_index(int task_id) {
    for (int i = 0; i < task_count; i++) {
        if (tasks[i].task_id == task_id) {
            return i;
        }
    }
    return -1;
}

void add_sample_tasks() {
    for (int i = 0; i < 20; i++) {
        tasks[task_count].task_id = i + 1;
        strcpy(tasks[task_count].operation, "PRIME");
        tasks[task_count].input = 300000 + i * 10000;
        tasks[task_count].status = TASK_READY;
        tasks[task_count].assigned_worker = -1;
        task_count++;
    }
}

int choose_worker() {
    int selected = -1;

    if (policy == POLICY_ROUND_ROBIN) {
        for (int i = 0; i < worker_count; i++) {
            int idx = (next_worker_rr + i) % worker_count;
            if (workers[idx].status == WORKER_ALIVE) {
                selected = idx;
                next_worker_rr = (idx + 1) % worker_count;
                break;
            }
        }
    } else if (policy == POLICY_LEAST_LOADED) {
        int min_load = 999999;
        for (int i = 0; i < worker_count; i++) {
            if (workers[i].status == WORKER_ALIVE &&
                workers[i].current_load < min_load) {
                min_load = workers[i].current_load;
                selected = i;
            }
        }
    } else {
        for (int i = 0; i < worker_count; i++) {
            if (workers[i].status == WORKER_ALIVE) {
                selected = i;
                break;
            }
        }
    }

    return selected;
}

void *scheduler_thread(void *arg) {
    while (1) {
        pthread_mutex_lock(&lock);

        for (int i = 0; i < task_count; i++) {
            if (tasks[i].status == TASK_READY) {
                int widx = choose_worker();

                if (widx != -1) {
                    char msg[BUFFER_SIZE];

                    snprintf(
                        msg,
                        sizeof(msg),
                        "TASK %d %s %d\n",
                        tasks[i].task_id,
                        tasks[i].operation,
                        tasks[i].input
                    );

                    send(workers[widx].socket_fd, msg, strlen(msg), 0);

                    tasks[i].status = TASK_RUNNING;
                    tasks[i].assigned_worker = workers[widx].worker_id;
                    workers[widx].current_load++;

                    printf("[SCHEDULER] Task %d assigned to Worker %d\n",
                           tasks[i].task_id,
                           workers[widx].worker_id);
                }
            }
        }

        pthread_mutex_unlock(&lock);

        sleep(1);
    }

    return NULL;
}

void *heartbeat_monitor_thread(void *arg) {
    while (1) {
        pthread_mutex_lock(&lock);

        long now = current_time_sec();

        for (int i = 0; i < worker_count; i++) {
            if (workers[i].status == WORKER_ALIVE &&
                now - workers[i].last_heartbeat > HEARTBEAT_TIMEOUT) {

                printf("[FAILURE] Worker %d failed\n", workers[i].worker_id);

                workers[i].status = WORKER_FAILED;
                workers[i].current_load = 0;

                for (int j = 0; j < task_count; j++) {
                    if (tasks[j].status == TASK_RUNNING &&
                        tasks[j].assigned_worker == workers[i].worker_id) {

                        tasks[j].status = TASK_READY;
                        tasks[j].assigned_worker = -1;

                        printf("[RECOVERY] Task %d moved back to READY\n",
                               tasks[j].task_id);
                    }
                }

                close(workers[i].socket_fd);
            }
        }

        pthread_mutex_unlock(&lock);

        sleep(1);
    }

    return NULL;
}

void handle_register(int client_fd, int worker_id, int cpu_cores) {
    pthread_mutex_lock(&lock);

    int idx = find_worker_index(worker_id);

    if (idx == -1) {
        workers[worker_count].worker_id = worker_id;
        workers[worker_count].socket_fd = client_fd;
        workers[worker_count].cpu_cores = cpu_cores;
        workers[worker_count].current_load = 0;
        workers[worker_count].status = WORKER_ALIVE;
        workers[worker_count].last_heartbeat = current_time_sec();

        worker_count++;

        printf("[REGISTER] Worker %d registered\n", worker_id);
    } else {
        workers[idx].socket_fd = client_fd;
        workers[idx].status = WORKER_ALIVE;
        workers[idx].last_heartbeat = current_time_sec();

        printf("[REGISTER] Worker %d reconnected\n", worker_id);
    }

    pthread_mutex_unlock(&lock);
}

void handle_heartbeat(int worker_id) {
    pthread_mutex_lock(&lock);

    int idx = find_worker_index(worker_id);
    if (idx != -1 && workers[idx].status == WORKER_ALIVE) {
        workers[idx].last_heartbeat = current_time_sec();
        printf("[HEARTBEAT] Worker %d\n", worker_id);
    }

    pthread_mutex_unlock(&lock);
}

void handle_result(int task_id, char *output) {
    pthread_mutex_lock(&lock);

    int tidx = find_task_index(task_id);

    if (tidx != -1) {
        tasks[tidx].status = TASK_COMPLETED;
        strncpy(tasks[tidx].output, output, sizeof(tasks[tidx].output) - 1);

        int worker_id = tasks[tidx].assigned_worker;
        int widx = find_worker_index(worker_id);

        if (widx != -1 && workers[widx].current_load > 0) {
            workers[widx].current_load--;
        }

        printf("[RESULT] Task %d completed. Output = %s\n",
               task_id,
               output);
    }

    pthread_mutex_unlock(&lock);
}

void *client_handler_thread(void *arg) {
    int client_fd = *(int *)arg;
    free(arg);

    char buffer[BUFFER_SIZE];

    while (1) {
        memset(buffer, 0, sizeof(buffer));

        int bytes = recv(client_fd, buffer, sizeof(buffer) - 1, 0);

        if (bytes <= 0) {
            close(client_fd);
            break;
        }

        char type[32];
        sscanf(buffer, "%s", type);

        if (strcmp(type, "REGISTER") == 0) {
            int worker_id, cpu_cores;
            sscanf(buffer, "REGISTER %d %d", &worker_id, &cpu_cores);
            handle_register(client_fd, worker_id, cpu_cores);
        } else if (strcmp(type, "HEARTBEAT") == 0) {
            int worker_id;
            sscanf(buffer, "HEARTBEAT %d", &worker_id);
            handle_heartbeat(worker_id);
        } else if (strcmp(type, "RESULT") == 0) {
            int task_id;
            char output[256];
            sscanf(buffer, "RESULT %d %s", &task_id, output);
            handle_result(task_id, output);
        }
    }

    return NULL;
}

void *accept_thread(void *arg) {
    int server_fd = *(int *)arg;

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(
            server_fd,
            (struct sockaddr *)&client_addr,
            &client_len
        );

        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        int *pclient = malloc(sizeof(int));
        *pclient = client_fd;

        pthread_t tid;
        pthread_create(&tid, NULL, client_handler_thread, pclient);
        pthread_detach(tid);
    }

    return NULL;
}

SchedulePolicy parse_policy(char *name) {
    if (strcmp(name, "fifo") == 0) return POLICY_FIFO;
    if (strcmp(name, "rr") == 0) return POLICY_ROUND_ROBIN;
    if (strcmp(name, "least") == 0) return POLICY_LEAST_LOADED;

    return POLICY_LEAST_LOADED;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf("Usage: %s <port> <fifo|rr|least>\n", argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);
    policy = parse_policy(argv[2]);

    add_sample_tasks();

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in server_addr;

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(server_fd, 10) < 0) {
        perror("listen");
        return 1;
    }

    printf("[MASTER] Listening on port %d\n", port);

    pthread_t accept_tid, scheduler_tid, heartbeat_tid;

    pthread_create(&accept_tid, NULL, accept_thread, &server_fd);
    pthread_create(&scheduler_tid, NULL, scheduler_thread, NULL);
    pthread_create(&heartbeat_tid, NULL, heartbeat_monitor_thread, NULL);

    pthread_join(accept_tid, NULL);
    pthread_join(scheduler_tid, NULL);
    pthread_join(heartbeat_tid, NULL);

    close(server_fd);

    return 0;
}