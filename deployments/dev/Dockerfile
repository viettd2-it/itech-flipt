# ----- Stage 1: Build binary and UI -----
FROM golang:1.24-alpine3.21 AS build

WORKDIR /home/flipt

# Install required build tools
RUN apk add --update --no-cache npm git bash gcc build-base binutils-gold

# Bootstrap Mage CLI (required by build)
RUN git clone https://github.com/magefile/mage && \
    cd mage && \
    go run bootstrap.go

# Copy Go mod files first for better layer caching
COPY go.mod .
COPY go.sum .

# Copy folders used in go.mod replace statements
COPY ./core ./core
COPY ./errors ./errors
COPY ./sdk ./sdk
COPY ./rpc/flipt ./rpc/flipt

# Download dependencies
RUN go mod download

# Now copy the rest of the source
COPY . .

# Enable CGo
ENV CGO_ENABLED=1

# Build binary and UI
RUN go install github.com/magefile/mage@latest
RUN mage build
RUN mage ui:build

# ----- Stage 2: Minimal runtime image -----
FROM alpine:3.19

LABEL maintainer="viettd2-it@github.com"
LABEL org.opencontainers.image.name="flipt-custom"
LABEL org.opencontainers.image.source="https://github.com/viettd2-it/itech-flipt"

# Install runtime dependencies
RUN apk add --update --no-cache postgresql-client openssl ca-certificates

# Create config and data directories
RUN mkdir -p /etc/flipt/config /etc/flipt/policy /var/opt/flipt /var/log/flipt

# Copy built Flipt binary from build stage
COPY --from=build /home/flipt/bin/flipt /

# Copy your config and policy files
COPY deployments/dev/flipt-config.yml /etc/flipt/config/flipt-config.yml
COPY deployments/opa-policy/policy.rego /etc/flipt/policy/policy.rego

# Create flipt user and set permissions
RUN addgroup flipt && \
    adduser -S -D -g '' -G flipt -s /bin/sh flipt && \
    chown -R flipt:flipt /etc/flipt /var/opt/flipt /var/log/flipt

# Set user and expose ports
USER flipt
EXPOSE 8080
EXPOSE 9000

# Default command
CMD ["./flipt", "serve", "--config", "/etc/flipt/config/flipt-config.yml"]
