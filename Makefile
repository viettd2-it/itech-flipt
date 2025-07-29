.PHONY: build backend frontend clean dev prod-build prod-run prod-deploy

## Backend Go binary path
BACKEND_BINARY=bin/flipt

# 👇 Rebuild backend binary
backend:
	go build -o $(BACKEND_BINARY) ./cmd/flipt

# 👇 Build production binary with optimizations
prod-build:
	CGO_ENABLED=1 go build -ldflags="-s -w" -o $(BACKEND_BINARY) ./cmd/flipt

# 👇 Run production binary
prod-run:
	./$(BACKEND_BINARY) --config ./deployments/prod/flipt-config.yml --force-migrate

# 👇 Deploy to production (build + copy files)
prod-deploy: prod-build
	./scripts/deploy-prod.sh

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
