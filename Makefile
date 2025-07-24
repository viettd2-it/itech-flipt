.PHONY: build backend frontend clean dev

## Backend Go binary path
BACKEND_BINARY=bin/flipt

# 👇 Rebuild backend binary
backend:
	go build -o $(BACKEND_BINARY) ./cmd/flipt

# 👇 Rebuild frontend React app
frontend:
	cd ui && npm install && npm run build

# 👇 Rebuild everything
build: backend frontend

# 👇 Clean build files
clean:
	rm -f $(BACKEND_BINARY)
	cd ui && rm -rf dist node_modules

dev:
	concurrently \
	"./bin/flipt --config ./config-v/flipt-config.yml --force-migrate" \
	"mage ui:dev"
