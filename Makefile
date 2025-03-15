DOCKER_CMD ?= docker
DOCKER_REGISTRY ?= docker.io
DOCKER_ORG ?= $(USER)
DOCKER_NAME ?= kaf
DOCKER_TAG ?= latest
BUILD_TAG ?= latest

build:
	go build -ldflags "-w -s" ./cmd/kaf
install:
	go install -ldflags "-w -s" ./cmd/kaf
release:
	goreleaser
run-kafka:
	docker-compose up -d
docker-build:
	${DOCKER_CMD} build -t ${DOCKER_REGISTRY}/${DOCKER_ORG}/${DOCKER_NAME}:${DOCKER_TAG} .
