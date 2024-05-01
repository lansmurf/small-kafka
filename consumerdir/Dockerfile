# Use Alpine as base image
FROM alpine:latest

# Install GCC, libc-dev (for basic C libraries), and Jansson
RUN apk add --no-cache gcc libc-dev jansson-dev make
RUN apk add --no-cache iputils

# Set the working directory
WORKDIR /usr/src/app

# Copy the source code into the image
COPY ./consumer.c .

# Compile the program
RUN gcc -o consumer consumer.c -ljansson

#CMD ["./consumer", "A", "A", "A", "true"]
