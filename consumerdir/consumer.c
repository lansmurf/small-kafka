#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <jansson.h>
#include <signal.h>
#include <sys/time.h>
#include <errno.h>
#include <netdb.h>

#define SERVER_PORT 8080

char *session_id = NULL;
volatile sig_atomic_t running = 1;
volatile sig_atomic_t disconnect_type = 0;  // 0: Unknown, 1: Graceful, 2: Ungraceful


void signal_handler(int sig) {
    running = 0;
    disconnect_type = 1;  // Graceful exit
}


int setup_socket() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }
    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(SERVER_PORT);

    struct hostent *he;
    he = gethostbyname("cluster");
    if (he == NULL) {
        herror("gethostbyname");
        exit(1);
    }
    server_address.sin_addr = *((struct in_addr *)he->h_addr_list[0]);

    inet_pton(AF_INET, &server_address.sin_addr, &server_address.sin_addr);
    if (connect(sock, (struct sockaddr*)&server_address, sizeof(server_address)) < 0) {
        perror("Connection failed");
        close(sock);
        exit(EXIT_FAILURE);
    }
    struct timeval tv;
    tv.tv_sec = 5;  // 5-second timeout
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

    return sock;
}

void send_initial_request(int sock, const char *topic, int persistent) {
    json_t *json = json_object();
    if (session_id) {
        json_object_set_new(json, "session_id", json_string(session_id));
    } else {
        json_t *topics_json = json_array();
        char *topic_copy = strdup(topic);
        char *token = strtok(topic_copy, ".");
        while (token) {
            json_array_append_new(topics_json, json_string(token));
            token = strtok(NULL, ".");
        }
        free(topic_copy);
        json_object_set_new(json, "topic_req", topics_json);
        json_object_set_new(json, "persistent", json_boolean(persistent));
    }
    char *json_string_var = json_dumps(json, 0);
    send(sock, json_string_var, strlen(json_string_var), 0);
    free(json_string_var);
    json_decref(json);
}

void handle_response(int sock, const char *topic, int persistent) {
    char buffer[4096] = {0};
    ssize_t bytes_received = recv(sock, buffer, sizeof(buffer), 0);
    if (bytes_received <= 0) {
        // Check if the receive call was interrupted by a signal
        if (errno == EINTR) {
            printf("Interrupted by signal, exiting...\n");
            running = 0;
            disconnect_type = 1;  // Graceful exit
        } else {
            printf("Connection lost or timed out. Attempting to reconnect...\n");
            disconnect_type = 2;  // Ungraceful exit
        }
        close(sock);
        if (session_id && persistent && disconnect_type != 1) {  // Only reconnect if it's not a graceful exit
            sleep(5);  // Wait 5 seconds before reconnection
        } else {
            running = 0;
        }
        return;
    }
    json_error_t error;
    json_t *root = json_loads(buffer, 0, &error);
    if (!root) {
        fprintf(stderr, "error: on line %d: %s\n", error.line, error.text);
        return;
    }
    json_t *session_id_json = json_object_get(root, "session_id");
    if (session_id_json && !session_id) {
        session_id = strdup(json_string_value(session_id_json));
    }
    json_t *data_json = json_object_get(root, "data");
    const char *data = json_string_value(data_json);
    printf("Received information for topic '%s':\nData: %s\n", topic, data);
    json_decref(root);
}

void request_topic(const char *topic, int persistent) {
    int sock = setup_socket(); // Initialize the socket here, before the loop
    send_initial_request(sock, topic, persistent);
    while (running) {
        if (disconnect_type == 1) {
            // Graceful disconnect, do not try to reconnect
            running = 0;
        } else if (disconnect_type == 2) {
            // Ungraceful disconnect, try to reconnect
            close(sock); // Close the old socket
            sock = setup_socket(); // Reinitialize the socket
            send_initial_request(sock, topic, persistent);
            disconnect_type = 0;  // Reset disconnect_type
        }
        
        handle_response(sock, topic, persistent);
    }

    close(sock); 
}

int main(int argc, char *argv[]) {
    signal(SIGINT, signal_handler);
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <topic chain> <persistent (true or false)>\n", argv[0]);
        return EXIT_FAILURE;
    }
    char *topic = argv[1];
    int persistent = (strcmp(argv[2], "true") == 0) ? 1 : 0;
    request_topic(topic, persistent);
    return EXIT_SUCCESS;
}
