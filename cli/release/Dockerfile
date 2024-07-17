FROM golang:latest

# Install dependencies
RUN apt-get update && apt-get install -y zip

# Set the working directory
WORKDIR /go/src/app

# Copy the entire project into the container
COPY .. .

# Install any needed packages specified in go.mod
RUN go mod download

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Build script
COPY release/build.sh /build.sh
RUN chmod +x /build.sh

# Use an environment variable for the version
ENV VERSION=dev

# Pass the VERSION to the build script
CMD ["/bin/sh", "-c", "/build.sh $VERSION"]