#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>

#define PORT 5678
#define BUFFER_SIZE 1024
#define MAX_KEY_VALUE_PAIRS 100
#define MAX_CLIENTS 100

typedef struct {
    char key[128];
    char value[128];
} KeyValuePair;

KeyValuePair store[MAX_KEY_VALUE_PAIRS];
int store_size = 0;
pthread_mutex_t store_mutex;
pthread_mutex_t transaction_mutex;
pthread_t transaction_owner;

typedef struct {
    int client_socket;
    char key[128];
} Subscriber;

Subscriber subscribers[MAX_CLIENTS];
int num_subscribers = 0;
pthread_mutex_t subscribers_mutex;

void *client_handler(void *client_socket_ptr);

void subscribe(int client_socket, const char *key);
void publish(const char *key, const char *value);

void subscribe(int client_socket, const char *key) {
    pthread_mutex_lock(&subscribers_mutex);
    if (num_subscribers >= MAX_CLIENTS) {
        // Handle error: Maximum number of subscribers reached
        pthread_mutex_unlock(&subscribers_mutex);
        return;
    }

    Subscriber new_subscriber;
    new_subscriber.client_socket = client_socket;
    strncpy(new_subscriber.key, key, sizeof(new_subscriber.key));

    subscribers[num_subscribers++] = new_subscriber;

    pthread_mutex_unlock(&subscribers_mutex);
}

void publish(const char *key, const char *value) {
    pthread_mutex_lock(&subscribers_mutex);

    char buffer[BUFFER_SIZE];
    snprintf(buffer, BUFFER_SIZE, "PUB:%s:%s\n", key, value);

    for (int i = 0; i < num_subscribers; i++) {
        Subscriber subscriber = subscribers[i];
        if (strcmp(subscriber.key, key) == 0) {
            send(subscriber.client_socket, buffer, strlen(buffer), 0);
        }
    }

    pthread_mutex_unlock(&subscribers_mutex);
}

void *client_handler(void *client_socket_ptr) {
    int client_socket = *(int *)client_socket_ptr;
    int in_transaction = 0;

    char buffer[BUFFER_SIZE];
    int bytes_received;
    while ((bytes_received = recv(client_socket, buffer, BUFFER_SIZE - 1, 0)) > 0) {
        buffer[bytes_received] = '\0';
        char cmd[16], key[128], value[128];
        sscanf(buffer, "%15s %127s %127s", cmd, key, value);

        if (strcasecmp(cmd, "BEG") == 0) {
            pthread_mutex_lock(&transaction_mutex);
            if (in_transaction == 0) {
                in_transaction = 1;
                transaction_owner = pthread_self();
                snprintf(buffer, BUFFER_SIZE, "BEG:transaction_started\n");
            } else {
                snprintf(buffer, BUFFER_SIZE, "BEG:transaction_already_started\n");
            }
        } else if (strcasecmp(cmd, "END") == 0) {
            if (in_transaction == 1 && pthread_equal(transaction_owner, pthread_self())) {
                in_transaction = 0;
                snprintf(buffer, BUFFER_SIZE, "END:transaction_ended\n");
                pthread_mutex_unlock(&transaction_mutex);
            } else {
                snprintf(buffer, BUFFER_SIZE, "END:no_transaction_to_end\n");
            }
        } else if (strcasecmp(cmd, "SUB") == 0) {
            subscribe(client_socket, key);
            snprintf(buffer, BUFFER_SIZE, "SUB:%s:key_subscribed\n", key);
        } else {
            if (in_transaction == 1 && !pthread_equal(transaction_owner, pthread_self())) {
                snprintf(buffer, BUFFER_SIZE, "ERROR:transaction_in_progress\n");
                send(client_socket, buffer, strlen(buffer), 0);
                continue;
            } else {
                if (!in_transaction) {
                    // Wait until the transaction is ended
                    pthread_mutex_lock(&transaction_mutex);
                    pthread_mutex_unlock(&transaction_mutex);
                }

                pthread_mutex_lock(&store_mutex);
                if (strcasecmp(cmd, "GET") == 0) {
                    int found = 0;
                    for (int i = 0; i < store_size; i++) {
                        if (strcmp(key, store[i].key) == 0) {
                            snprintf(buffer, BUFFER_SIZE, "GET:%s:%s\n", key, store[i].value);
                            found = 1;
                            break;
                        }
                    }
                    if (!found)
                        snprintf(buffer, BUFFER_SIZE, "GET:%s:key_nonexistent\n", key);
                } else if (strcasecmp(cmd, "PUT") == 0) {
                    int index = -1;
                    for (int i = 0; i < store_size; i++) {
                        if (strcmp(key, store[i].key) == 0) {
                            index = i;
                            break;
                        }
                    }
                    if (index == -1) {
                        index = store_size++;
                        strncpy(store[index].key, key, sizeof(store[index].key));
                    }
                    strncpy(store[index].value, value, sizeof(store[index].value));
                    snprintf(buffer, BUFFER_SIZE, "PUT:%s:%s\n", key, value);
                    publish(key, value);
                } else if (strcasecmp(cmd, "DEL") == 0) {
                    int found = 0;
                    for (int i = 0; i < store_size; i++) {
                        if (strcmp(key, store[i].key) == 0) {
                            store[i] = store[--store_size];
                            snprintf(buffer, BUFFER_SIZE, "DEL:%s:key_deleted\n", key);
                            found = 1;
                            break;
                        }
                    }
                    if (!found)
                        snprintf(buffer, BUFFER_SIZE, "DEL:%s:key_nonexistent\n", key);
                } else if (strcasecmp(cmd, "QUIT") == 0) {
                    pthread_mutex_unlock(&store_mutex);
                    break;
                }
                pthread_mutex_unlock(&store_mutex);
            }
        }
        send(client_socket, buffer, strlen(buffer), 0);
    }
    close(client_socket);
    free(client_socket_ptr);
    return NULL;
}

int main() {
    int server_socket;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_size = sizeof(client_addr);

    server_socket = socket(AF_INET, SOCK_STREAM, 0);

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr));
    listen(server_socket, MAX_CLIENTS);
    printf("Server listening on port %d\n", PORT);

    pthread_mutex_init(&store_mutex, NULL);
    pthread_mutex_init(&transaction_mutex, NULL);
    pthread_mutex_init(&subscribers_mutex, NULL);

    while (1) {
        int *client_socket_ptr = malloc(sizeof(int));
        *client_socket_ptr = accept(server_socket, (struct sockaddr *)&client_addr, &client_addr_size);

        pthread_t client_thread;
        pthread_create(&client_thread, NULL, client_handler, client_socket_ptr);
        pthread_detach(client_thread);
    }

    pthread_mutex_destroy(&store_mutex);
    pthread_mutex_destroy(&transaction_mutex);
    pthread_mutex_destroy(&subscribers_mutex);

    return 0;
}
