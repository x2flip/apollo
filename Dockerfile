# Start with Rust official image to build the application.
FROM rust:1.68 as builder

# Create a new empty shell project and build dependencies.
# This step is done to cache dependencies and only rebuilds them when they change.
WORKDIR /usr/src
RUN USER=root cargo new apollo
WORKDIR /usr/src/apollo
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release

# Copy the source code and build the application.
COPY src ./src
RUN touch src/main.rs && cargo build --release

# Start a new build stage: this will reduce the image size 
# by leaving out build dependencies and intermediate artifacts.
FROM debian:bullseye

# Install SSL certificates.
RUN apt-get update \
    && apt-get install -y ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y libssl-dev && \
    rm -rf /var/lib/apt/lists/*


# Copy the build artifact from the builder stage.
COPY --from=builder /usr/src/apollo/target/release/apollo /usr/local/bin/apollo

# Set the start command.
CMD ["apollo"]

# Expose port 8080 to the outside world.
EXPOSE 8081

