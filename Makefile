.PHONY: test test-coverage lint tidy

test: test-coverage

test-coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

lint:
	go vet ./...

tidy:
	go mod tidy
