.PHONY: test test-coverage lint tidy example

test: test-coverage

test-coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

lint:
	go vet ./...

tidy:
	go mod tidy

example:
	@echo "Running asynx E-Commerce Order example..."
	@cd example && go run .
