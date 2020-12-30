build:
	go build -ldflags "-w -s" ./cmd/kaf
install:
	go install -ldflags "-w -s" ./cmd/kaf
release:
	goreleaser --rm-dist
run-kafka:
	docker-compose up -d
