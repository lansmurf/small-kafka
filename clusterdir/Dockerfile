# Use Alpine as base image
FROM alpine:latest

# Install necessary packages
RUN apk add --no-cache \
    gcc \
    libc-dev \
    jansson-dev \
    make \
    curl \
    wget

# Create the directory for the header file
RUN mkdir -p /usr/local/include/

# Download uthash.h from its GitHub repository
RUN wget https://raw.githubusercontent.com/troydhanson/uthash/master/src/uthash.h -O /usr/local/include/uthash.h

# Set the working directory
WORKDIR /usr/src/app

# Copy the source code into the image
COPY ./cluster.c .

# Compile the program
RUN gcc -o cluster cluster.c -lpthread -ljansson -lm

# Command to run the compiled binary
# CMD ["./cluster"]
