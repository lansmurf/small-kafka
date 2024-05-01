#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <jansson.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/time.h>
#include <uthash.h>
#include <math.h>


#define NUM_ALLOWED_TOPICS 26

int total_partitions;
int num_brokers;

typedef struct Partition Partition;
typedef struct Topic Topic;
typedef struct Broker Broker;
typedef struct Cluster Cluster;

struct Partition {
    int id;
    char **messages;  
    int message_count;
    int capacity;
    pthread_mutex_t lock;
};

struct Topic {
    char *name;
    UT_hash_handle hh; 
    Partition *partitions;
    int num_partitions;
    int last_assigned_partition;
    struct Topic *children;  
};

struct Broker {
    int id;
    Topic* topics;  
};

struct Cluster {
    Broker* brokers;
    int num_brokers;
    int last_assigned_broker;
    pthread_mutex_t last_assigned_broker_lock;
};


typedef struct {
    char **topic_chain;  
    int topic_count;   
    char *message;
    int partition_key;
} Message;


// Parsing function for consumer
typedef struct {
    char **topic_chain;
    int topic_count;
    int persistent;
    char *session_id;
} Consumer;

Consumer consumers[200];
int consumer_count;

typedef struct {
    int client_socket;
    Cluster *cluster; // Using the forward-declared Cluster
} ThreadArgs;

pthread_mutex_t round_robin_lock;
int current_partition = 0; 

const char* ALLOWED_TOPICS[] = {
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
    "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
};

// Function to initialize partitions
Partition* initialize_partitions(int partition_count, int* partition_id_counter) {
    Partition* partitions = (Partition*)malloc(sizeof(Partition) * partition_count);
    for (int i = 0; i < partition_count; ++i) {
        partitions[i].id = (*partition_id_counter)++;
        partitions[i].message_count = 0;
        partitions[i].capacity = 50;  // Set the maximum capacity
        pthread_mutex_init(&partitions[i].lock, NULL);
        partitions[i].messages = (char**)malloc(50 * sizeof(char*));
    }
    return partitions;
}

// Function to initialize a single topic with or without its partitions
Topic* initialize_topic(int partition_count, int* partition_id_counter, int isLeaf) {
    Topic* topic = (Topic*)malloc(sizeof(Topic));
    if (isLeaf) {
        topic->partitions = initialize_partitions(partition_count, partition_id_counter);
        topic->num_partitions = partition_count;
    } else {
        topic->partitions = NULL;
        topic->num_partitions = 0;
    }
    topic->children = NULL;  // Initialize hash table to NULL
    return topic;
}

// Recursive function to initialize topics at different levels
void recursive_initialize_topic(Topic** parent, int depth, int width, int partition_count, int* partition_id_counter) {
    if (depth <= 0) return;
    for (int i = 0; i < width; i++) {
        char* topic_name = (char*)malloc(10 * sizeof(char));
        sprintf(topic_name, "T%d%d", depth, i);

        Topic* new_topic = initialize_topic(partition_count, partition_id_counter, depth == 1);
        new_topic->name = topic_name;
        
        HASH_ADD_KEYPTR(hh, *parent, new_topic->name, strlen(new_topic->name), new_topic);

        recursive_initialize_topic(&new_topic->children, depth-1, width, partition_count, partition_id_counter);
    }
}

// Function to initialize brokers and cluster
Cluster* initialize_cluster(int num_brokers, int depth, int width, int partition_count) {
    Cluster* cluster = (Cluster*)malloc(sizeof(Cluster));
    cluster->brokers = (Broker*)malloc(sizeof(Broker) * num_brokers);
    cluster->num_brokers = num_brokers;
    
    int partition_id_counter = 0;

    for (int broker_id = 0; broker_id < num_brokers; broker_id++) {
        Broker* broker = &cluster->brokers[broker_id];
        broker->id = broker_id;
        broker->topics = NULL; 

        // Initialize top-level topics
        recursive_initialize_topic(&broker->topics, depth, width, partition_count, &partition_id_counter);
    }
    print_cluster(cluster);
    return cluster;
}


// Function to print partitions
void print_partitions(Partition* partitions, int num_partitions) {
    for (int i = 0; i < num_partitions; ++i) {
        printf("            Partition ID: %d\n", partitions[i].id);
    }
}

// Recursive function to print topics and their subtopics
void print_topic(Topic* topic, int depth) {
    // Print indentation based on depth
    for (int i = 0; i < depth; ++i) {
        printf("  ");
    }

    printf("Topic Name: %s\n", topic->name);
    print_partitions(topic->partitions, topic->num_partitions);

    // Print subtopics, if any
    Topic *subtopic, *tmp;
    HASH_ITER(hh, topic->children, subtopic, tmp) {
        print_topic(subtopic, depth + 1);
    }
}

// Function to print the cluster
void print_cluster(Cluster* cluster) {
    printf("Cluster\n");
    for (int i = 0; i < cluster->num_brokers; ++i) {
        printf("Broker ID: %d\n", cluster->brokers[i].id);

        // Print topics
        Topic *topic, *tmp;
        HASH_ITER(hh, cluster->brokers[i].topics, topic, tmp) {
            print_topic(topic, 1);
        }
    }
}


Message parse_producer_message(const char *json_str) {
    Message message = {0};
    json_error_t error;
    json_t *root = json_loads(json_str, 0, &error);

    if (!root) {
        fprintf(stderr, "error: on line %d: %s\n", error.line, error.text);
        return message;
    }

    json_t *topics_json = json_object_get(root, "topics");
    json_t *key_json = json_object_get(root, "key");
    json_t *message_json = json_object_get(root, "message");

    if (!json_is_array(topics_json) || !json_is_integer(key_json) || !json_is_string(message_json)) {
        fprintf(stderr, "error: unexpected JSON types\n");
        json_decref(root);
        return message;
    }

    size_t topic_count = json_array_size(topics_json);
    char **topic_chain = malloc(topic_count * sizeof(char *));
    for (size_t i = 0; i < topic_count; ++i) {
        json_t *topic = json_array_get(topics_json, i);
        topic_chain[i] = strdup(json_string_value(topic));
    }

    message.topic_chain = topic_chain;
    message.topic_count = topic_count;
    message.partition_key = json_integer_value(key_json);
    message.message = strdup(json_string_value(message_json));

    printf("Parsed producer message:\n");
    printf("  Topic Chain: ");
    for (size_t i = 0; i < topic_count; ++i) {
        printf("%s ", message.topic_chain[i]);
    }
    printf("\n");
    printf("  Topic count: %d\n", message.topic_count);
    printf("  Key: %d\n", message.partition_key);
    printf("  Message: %s\n", message.message);
    json_decref(root);

    return message;
}


void free_message(Message *message) {
    for (int i = 0; i < message->topic_count; i++) {
        free(message->topic_chain[i]);
    }
    free(message->topic_chain);
    free(message->message);
}


Broker* get_broker_by_id(Cluster *cluster, int id) {
    int partitions_per_broker = total_partitions / num_brokers;
    int target_broker_id = id / partitions_per_broker;
    
    if (target_broker_id >= cluster->num_brokers) {
        target_broker_id = cluster->num_brokers - 1;
    }
    
    printf("Debug: Assigning partition %d to Broker %d\n", id, target_broker_id);
    return &cluster->brokers[target_broker_id];
}



Partition* find_partition_by_id(Partition *partitions, int num_partitions, int id) {
    for (int i = 0; i < num_partitions; i++) {
        if (partitions[i].id == id) {
            return &partitions[i];
        }
    }
    printf("Debug: Partition with ID %d was not found.\n", id);
    return NULL;
}

int find_brokers_for_topic(Cluster* cluster, char** topic_chain, int topic_count, int* broker_ids) {
    int broker_count = 0;
    
    for (int i = 0; i < cluster->num_brokers; i++) {
        Broker *broker = &cluster->brokers[i];
        Topic *current_topic = broker->topics;
        
        int topic_found = 1;
        
        for (int j = 0; j < topic_count; j++) {
            Topic *found_topic = NULL;
            HASH_FIND_STR(current_topic, topic_chain[j], found_topic);
            
            if (!found_topic) {
                topic_found = 0;
                break;
            }
            
            if (j == topic_count - 1) {
                if (found_topic->children != NULL) {
                    topic_found = 0;
                }
            } else {
                current_topic = found_topic->children;
            }
        }
        
        if (topic_found) {
            broker_ids[broker_count++] = broker->id;
            }
    }

    return broker_count;
}


int add_message(Cluster* cluster, Message message) {
    Topic *current_topic = NULL;
    Partition *partition = NULL;
    Broker *broker = NULL;

    // Broker selection logic
    int selected_broker_id;
    if (message.partition_key == -1) {
        int broker_ids[cluster->num_brokers];
        int found_brokers = find_brokers_for_topic(cluster, message.topic_chain, message.topic_count, broker_ids);

        if (found_brokers == 0) {
            return -7;
        }

        pthread_mutex_lock(&cluster->last_assigned_broker_lock);
        cluster->last_assigned_broker = (cluster->last_assigned_broker + 1) % found_brokers;
        selected_broker_id = broker_ids[cluster->last_assigned_broker];
        pthread_mutex_unlock(&cluster->last_assigned_broker_lock);

        broker = &cluster->brokers[selected_broker_id];
    } else {
        broker = get_broker_by_id(cluster, message.partition_key);
        if (!broker) {
            return -1;
        }
    }

    current_topic = broker->topics;  
    for (int i = 0; i < message.topic_count; i++) {
        Topic *found_topic = NULL;
        HASH_FIND_STR(current_topic, message.topic_chain[i], found_topic);
        if (!found_topic) {
            return -2;
        }
        current_topic = i == message.topic_count - 1 ? found_topic : found_topic->children;
        if (!current_topic) {
            return -5;
        }
    }

    // Partition logic
    if (current_topic->children != NULL) {
        return -5;
    }

    if (message.partition_key == -1) {
        printf("Selected Broker ID: %d\n", selected_broker_id);
        current_topic->last_assigned_partition = (current_topic->last_assigned_partition + 1) % current_topic->num_partitions;
        partition = &current_topic->partitions[current_topic->last_assigned_partition];
    } else {
        printf("Broker ID (from partition key): %d\n", message.partition_key);
        partition = find_partition_by_id(current_topic->partitions, current_topic->num_partitions, message.partition_key);
        if (!partition) {
            return -3;
        }
    }

    printf("Selected Partition ID: %d\n", partition->id);

    pthread_mutex_lock(&partition->lock);
    if (partition->message_count >= partition->capacity) {
        pthread_mutex_unlock(&partition->lock);
        return -4;
    }

    char *new_message = strdup(message.message);
    if (new_message == NULL) {
        pthread_mutex_unlock(&partition->lock);
        return -6;
    }
    partition->messages[partition->message_count++] = new_message;
    pthread_mutex_unlock(&partition->lock);

    return 0;
}



int create_server_socket(int port) {
    int server_fd;
    struct sockaddr_in address;

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    return server_fd;
}

char *generate_unique_session_id() {
    time_t timestamp = time(NULL);
    int random_number = rand();

    char *session_id = (char *)malloc(32 * sizeof(char));
    if (session_id == NULL) {
        fprintf(stderr, "Memory allocation for session ID failed\n");
        return NULL; 
    }

    snprintf(session_id, 32, "%ld%06d", timestamp, random_number % 1000000);

    return session_id;
}

Consumer parse_consumer_message(const char *json_str) {
    Consumer message = {0};  
    json_error_t error;
    json_t *root = json_loads(json_str, 0, &error);

    message.session_id = generate_unique_session_id();

    if (!root) {
        fprintf(stderr, "error: on line %d: %s\n", error.line, error.text);
        return message; 
    }

    json_t *topic_json = json_object_get(root, "topic_req");
    json_t *persistent_json = json_object_get(root, "persistent");

    if (!json_is_array(topic_json) || !json_is_boolean(persistent_json)) {
        fprintf(stderr, "error: unexpected JSON types\n");
        json_decref(root);
        return message;
    }

    size_t topic_count = json_array_size(topic_json);
    char **topic_chain = malloc(topic_count * sizeof(char *));
    for (size_t i = 0; i < topic_count; ++i) {
        json_t *topic = json_array_get(topic_json, i);
        topic_chain[i] = strdup(json_string_value(topic));
    }

    message.topic_chain = topic_chain;
    message.topic_count = topic_count;
    message.persistent = json_boolean_value(persistent_json) ? 1 : 0;

    printf("Parsed consumer message:\n");
    printf("  Topic Chain Request: ");
    for (size_t i = 0; i < topic_count; ++i) {
        printf("%s ", message.topic_chain[i]);
    }
    printf("\n");
    printf("  Persistent?: %d\n", message.persistent);

    json_decref(root);

    return message;
}


void gather_topic_messages(Topic* topic, char** buffer_ptr, int* buffer_size) {
    char* buffer = *buffer_ptr;
    for (int partition_index = 0; partition_index < topic->num_partitions; partition_index++) {
        Partition* partition = &topic->partitions[partition_index];
        for (int msg_index = 0; msg_index < partition->message_count; msg_index++) {
            int needed_space = strlen(buffer) + strlen(partition->messages[msg_index]) + 2;
            if (needed_space > *buffer_size) {
                *buffer_size *= 2;
                char* new_buffer = (char*)realloc(buffer, *buffer_size);
                if (!new_buffer) {
                    fprintf(stderr, "Debug: Reallocation failed\n");
                    free(buffer);
                    exit(1);
                }
                buffer = new_buffer;
                *buffer_ptr = buffer;
            }
            strcat(buffer, partition->messages[msg_index]);
            strcat(buffer, "\n");
        }
    }
}

void gather_all_messages_recursive(Topic *topic, char** buffer_ptr, int* buffer_size) {
    if (topic == NULL) {
        printf("Debug: Topic is NULL. Returning.\n");
        return;
    }

    gather_topic_messages(topic, buffer_ptr, buffer_size);

    Topic *current_child, *tmp;
    int child_count = HASH_COUNT(topic->children);
    HASH_ITER(hh, topic->children, current_child, tmp) {
        gather_all_messages_recursive(current_child, buffer_ptr, buffer_size);
    }
}


char* gather_information(Cluster* cluster, Consumer request) {
    int buffer_size = 1024;
    char* buffer = (char*)malloc(buffer_size);
    if (!buffer) {
        fprintf(stderr, "Debug: Memory allocation failed\n");
        return NULL;
    }
    buffer[0] = '\0';

    for (int broker_id = 0; broker_id < cluster->num_brokers; broker_id++) {
        Broker* broker = &cluster->brokers[broker_id];
        Topic *current_topic = broker->topics;

        for (int i = 0; i < request.topic_count; i++) {
            Topic *found_topic = NULL;

            if (strcmp(request.topic_chain[i], "#") == 0) {
                Topic *tmp, *current_child;
                HASH_ITER(hh, current_topic, current_child, tmp) {
                    gather_all_messages_recursive(current_child, &buffer, &buffer_size);
                }
                break;
            }

            HASH_FIND_STR(current_topic, request.topic_chain[i], found_topic);
            if (found_topic) {
                current_topic = found_topic->children;
            } else {
                fprintf(stderr, "Debug: Topic %s not found.\n", request.topic_chain[i]);
                return NULL;
            }

            if (i == request.topic_count - 1 && found_topic) {
                gather_all_messages_recursive(found_topic, &buffer, &buffer_size);
            }
        }
    }
    return buffer;
}

void *handle_client_connection(void *arg) {
    printf("Handling new client connection.\n");

    ThreadArgs *args = (ThreadArgs *)arg;
    int client_socket = args->client_socket;
    Cluster *cluster = args->cluster;

    char buffer[1024] = {0};
    read(client_socket, buffer, 1024);
    printf("Received initial message: %s\n", buffer);

    json_error_t error;
    json_t *root = json_loads(buffer, 0, &error);
    if (!root) {
        fprintf(stderr, "JSON error: on line %d: %s\n", error.line, error.text);
        free(args);
        close(client_socket);
        return NULL;
    }

    json_t *session_id_json = json_object_get(root, "session_id");
    Consumer *existing_consumer = NULL;

    if (session_id_json) {
        const char *incoming_session_id = json_string_value(session_id_json);
        for (int i = 0; i < consumer_count; ++i) {
            if (strcmp(consumers[i].session_id, incoming_session_id) == 0) {
                printf("Found a matching session for reconnection.\n");
                existing_consumer = &consumers[i];
                break;
            }
        }
    }

    if (json_object_get(root, "topic_req") || existing_consumer) {
        printf("Consumer identified.\n");

        
        Consumer req = existing_consumer ? *existing_consumer : parse_consumer_message(buffer);

        if (!existing_consumer) {
            req.session_id = generate_unique_session_id(); 

            consumers[consumer_count++] = req;
        }

        json_t *json_response = json_object();
        
        if (req.persistent) {
            while (1) {
                printf("Entering while loop\n");
                fd_set set;
                struct timeval timeout;
                int rv;

                FD_ZERO(&set);
                FD_SET(client_socket, &set);

                timeout.tv_sec = 3; 
                timeout.tv_usec = 0;

                rv = select(client_socket + 1, &set, NULL, NULL, &timeout);

                if (rv == -1) {
                    perror("select");
                    break;
                } else if (rv == 0) {
                    
                } else {
                    char check_buffer[10];
                    ssize_t r = read(client_socket, check_buffer, sizeof(check_buffer));
                    if (r == 0) {
                        printf("Client disconnected.\n");
                        break; 
                    }
                }

                char *response = gather_information(cluster, req);
                json_object_set_new(json_response, "session_id", json_string(req.session_id));
                json_object_set_new(json_response, "data", json_string(response));
                char *json_response_str = json_dumps(json_response, 0);


                if (send(client_socket, json_response_str, strlen(json_response_str), 0) == -1) {
                    perror("Send failed"); 
                    free(json_response_str);
                    free(response);
                    break;
                } else {
                    printf("Data sent to persistent consumer.\n");
                }

                free(json_response_str);
                free(response);      
            }
            printf("Exiting while loop\n");
        } else {
            printf("Non-persistent consumer.\n");
            char *response = gather_information(cluster, req);
            json_object_set_new(json_response, "data", json_string(response));
            char *json_response_str = json_dumps(json_response, 0);
            send(client_socket, json_response_str, strlen(json_response_str), 0);
            free(json_response_str);
            free(response);
        }

        json_decref(json_response);
        free(req.topic_chain);
        free(args);
        close(client_socket);
        printf("Closed connection with consumer.\n");
    } else {
        printf("Producer identified.\n");
        Message message = parse_producer_message(buffer);
        add_message(cluster, message);
        free(args);
        close(client_socket);
        printf("Closed connection with producer.\n");
    }

    printf("aqui??????");
    json_decref(root);

    return NULL;
}

void start_cluster_server(int server_socket, Cluster* cluster) { 
    while (1) {
        int client_socket;
        struct sockaddr_in client_address;
        int addrlen = sizeof(client_address);

        client_socket = accept(server_socket, (struct sockaddr *)&client_address, (socklen_t*)&addrlen);

        ThreadArgs *args = malloc(sizeof(ThreadArgs));
        args->client_socket = client_socket;
        args->cluster = cluster;

        pthread_t thread;
        if (pthread_create(&thread, NULL, (void *)handle_client_connection, (void *)args) != 0) {
            perror("Failed to create thread");
            continue;
        }

        if (pthread_detach(thread) != 0) {
            perror("Failed to detach thread");
            continue;
        }

        pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    }
}


int main() {
    int port = 8080;
    int server_socket = create_server_socket(port);

    num_brokers = 2;
    int depth = 3;
    int width = 3;
    int partitions_per_leaf = 3;

    total_partitions = (int)pow(width, depth) * partitions_per_leaf * num_brokers;

    Cluster* cluster = initialize_cluster(num_brokers, depth, width, partitions_per_leaf);

    start_cluster_server(server_socket, cluster);

    return 0;
}