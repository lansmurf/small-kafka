# Use Alpine as base image
FROM alpine:latest

# Install GCC, libc-dev (for basic C libraries), and Jansson
RUN apk add --no-cache gcc libc-dev jansson-dev make
RUN apk add --no-cache iputils

# Set the working directory
WORKDIR /usr/src/app

# Copy the source code into the image
COPY ./client.c .

# Compile the program
RUN gcc -o client client.c -ljansson

#CMD ["./client", "A", "A", "A", "1", "Testing"]
