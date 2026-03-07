.PHONY: test test-coverage lint tidy

test:
	go test -race ./...

test-coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

lint:
	go vet ./...

tidy:
	go mod tidy
