.PHONY: test build web-build run

test:
	go test ./...

web-build:
	cd web && npm run build

build: web-build
	rm -rf internal/ui/dist
	mkdir -p internal/ui/dist
	cp -R web/dist/. internal/ui/dist/
	go build -buildvcs=false -o mq-lens ./cmd/mq-lens

run:
	go run ./cmd/mq-lens
