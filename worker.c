#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>

#include "common.h"

int sock_fd;
int worker_id;

int is_prime(int n) {
    if (n < 2) return 0;

    for (int i = 2; i * i <= n; i++) {
        if (n % i == 0) return 0;
    }

    return 1;
}

int count_primes(int n) {
    int count = 0;

    for (int i = 2; i <= n; i++) {
        if (is_prime(i)) {
            count++;
        }
    }

    return count;
}

void *heartbeat_thread(void *arg) {
    while (1) {
        char msg[BUFFER_SIZE];

        snprintf(msg, sizeof(msg), "HEARTBEAT %d\n", worker_id);
        send(sock_fd, msg, strlen(msg), 0);

        sleep(HEARTBEAT_INTERVAL);
    }

    return NULL;
}

void execute_task(int task_id, char *operation, int input) {
    char result[256];

    printf("[WORKER %d] Executing Task %d: %s %d\n",
           worker_id,
           task_id,
           operation,
           input);

    if (strcmp(operation, "PRIME") == 0) {
        int output = count_primes(input);
        snprintf(result, sizeof(result), "%d", output);
    } else {
        snprintf(result, sizeof(result), "UNKNOWN_TASK");
    }

    char msg[BUFFER_SIZE];

    snprintf(msg, sizeof(msg), "RESULT %d %s\n", task_id, result);
    send(sock_fd, msg, strlen(msg), 0);

    printf("[WORKER %d] Task %d completed. Result = %s\n",
           worker_id,
           task_id,
           result);
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: %s <master_ip> <port> <worker_id>\n", argv[0]);
        return 1;
    }

    char *master_ip = argv[1];
    int port = atoi(argv[2]);
    worker_id = atoi(argv[3]);

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in master_addr;

    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(port);
    inet_pton(AF_INET, master_ip, &master_addr.sin_addr);

    if (connect(sock_fd, (struct sockaddr *)&master_addr, sizeof(master_addr)) < 0) {
        perror("connect");
        return 1;
    }

    printf("[WORKER %d] Connected to master\n", worker_id);

    char register_msg[BUFFER_SIZE];

    snprintf(register_msg, sizeof(register_msg), "REGISTER %d %d\n", worker_id, 4);
    send(sock_fd, register_msg, strlen(register_msg), 0);

    pthread_t hb_tid;
    pthread_create(&hb_tid, NULL, heartbeat_thread, NULL);
    pthread_detach(hb_tid);

    char buffer[BUFFER_SIZE];

    while (1) {
        memset(buffer, 0, sizeof(buffer));

        int bytes = recv(sock_fd, buffer, sizeof(buffer) - 1, 0);

        if (bytes <= 0) {
            printf("[WORKER %d] Master disconnected\n", worker_id);
            break;
        }

        char type[32];

        sscanf(buffer, "%s", type);

        if (strcmp(type, "TASK") == 0) {
            int task_id;
            char operation[32];
            int input;

            sscanf(buffer, "TASK %d %s %d", &task_id, operation, &input);

            execute_task(task_id, operation, input);
        }
    }

    close(sock_fd);

    return 0;
}