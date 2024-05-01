#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <jansson.h>
#include <netdb.h>

char *readCpuUsage() {
    FILE *f = fopen("/sys/fs/cgroup/cpu.stat", "r");
    if (f == NULL) {
        perror("Failed to open CPU stat file");
        exit(1);
    }

    long usage_usec, user_usec, system_usec;
    char buffer[256];
    snprintf(buffer, sizeof(buffer), "usage_usec %ld user_usec %ld system_usec %ld",
        usage_usec, user_usec, system_usec);
    fclose(f);

    char *result = strdup(buffer);
    return result;
}

char *readMemUsage() {
    FILE *f = fopen("/sys/fs/cgroup/memory.current", "r");
    if (f == NULL) {
        perror("Failed to open memory usage file");
        exit(1);
    }

    long usage;
    fscanf(f, "%ld", &usage);
    fclose(f);

    char *result = (char *)malloc(20);
    snprintf(result, 20, "%ld", usage);
    return result;
}

int main(int argc, char *argv[]) {
    if (argc != 4) {  
        fprintf(stderr, "Usage: %s <topic_chain> <key> <message>\n", argv[0]);
        return 1;
    }

    int sock;
    struct sockaddr_in server_address;

    // Create socket
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        perror("Could not create socket");
        return 1;
    }

    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(8080);
    struct hostent *he;
    he = gethostbyname("cluster");
    if (he == NULL) {
        herror("gethostbyname");
        exit(1);
    }
    server_address.sin_addr = *((struct in_addr *)he->h_addr_list[0]);

    // Connect to broker
    if (connect(sock, (struct sockaddr *)&server_address, sizeof(server_address)) < 0) {
        perror("Connection failed");
        return 1;
    }

    // Get system info based on the last argument
    char *system_info_str;

    if (strcmp(argv[3], "CPU") == 0) {
        system_info_str = readCpuUsage();
    } else if (strcmp(argv[3], "MEM") == 0) {
        system_info_str = readMemUsage();
    } else {
        fprintf(stderr, "Invalid system info key. Choose 'CPU' or 'MEM'.\n");
        return 1;
    }

    // Prepare JSON message
    json_t *json = json_object();

    // Tokenize topic chain into JSON array
    json_t *topics_json = json_array();
    char *token = strtok(argv[1], ".");
    while (token != NULL) {
        json_array_append_new(topics_json, json_string(token));
        token = strtok(NULL, ".");
    }

    json_object_set_new(json, "topics", topics_json);
    json_object_set_new(json, "key", json_integer(atoi(argv[2])));
    json_object_set_new(json, "message", json_string(system_info_str));

    char *message = json_dumps(json, 0);

    // Send message
    if (send(sock, message, strlen(message), 0) < 0) {
        perror("Send failed");
        return 1;
    }

    free(message);
    json_decref(json);

    close(sock);
    printf("Message sent\n");

    return 0;
}
