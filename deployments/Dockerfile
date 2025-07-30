# Build stage
FROM golang:1.24-alpine AS build

WORKDIR /home/flipt

# Install build dependencies
RUN apk add --update --no-cache npm git bash gcc build-base binutils-gold

# Bootstrap Mage CLI
RUN git clone https://github.com/magefile/mage && \
    cd mage && \
    go run bootstrap.go

# Copy Go mod files first
COPY go.mod go.sum ./
COPY ./core ./core
COPY ./errors ./errors
COPY ./sdk ./sdk
COPY ./rpc/flipt ./rpc/flipt

RUN go mod download
COPY . .

ENV CGO_ENABLED=1
RUN go install github.com/magefile/mage@latest
RUN mage build
RUN mage ui:build

# Runtime stage
FROM alpine:3.19

LABEL maintainer="viettd2-it@github.com"
LABEL org.opencontainers.image.name="feature-flag-service"

RUN apk add --update --no-cache postgresql-client openssl ca-certificates

RUN mkdir -p /etc/flipt/config /etc/flipt/policy /var/opt/flipt /var/log/flipt

COPY --from=build /home/flipt/bin/flipt /

# Copy PRODUCTION config by default
#COPY deployments/flipt-config.prod.yml /etc/flipt/config/flipt-config.yml.template
COPY deployments/flipt-config.local.yml /etc/flipt/config/flipt-config.yml.template
COPY deployments/opa-policy/policy.rego /etc/flipt/policy/policy.rego

# Simple startup script (no envsubst needed)
RUN cat > /start.sh << 'EOF'
#!/bin/sh
echo "Starting Flipt..."
exec /flipt --force-migrate --config /etc/flipt/config/flipt-config.yml
EOF

RUN chmod +x /start.sh

RUN addgroup flipt && \
    adduser -S -D -g '' -G flipt -s /bin/sh flipt && \
    chown -R flipt:flipt /etc/flipt /var/opt/flipt /var/log/flipt /start.sh

USER flipt
EXPOSE 8080 9000

CMD ["/start.sh"]
