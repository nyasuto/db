.PHONY: all test bench clean fmt vet lint help

# Go parameters
GOCMD=go
GOTEST=$(GOCMD) test
GOFMT=$(GOCMD) fmt
GOMOD=$(GOCMD) mod
GOLINT=golangci-lint

# Target directories
TEST_PKGS=./internal/...

all: tidy test

test: ## 競合状態のチェック(-race)を含めてテストを実行します
	$(GOTEST) -v -race $(TEST_PKGS)

bench: ## ベンチマークを実行します
	$(GOTEST) -v -bench=. -benchmem $(TEST_PKGS)

fmt: ## コードをフォーマットします
	$(GOFMT) ./...

tidy: ## go.mod の依存関係を整理します
	$(GOMOD) tidy

lint: ## golangci-lint を実行します (brew install golangci-lint などでインストールが必要)
	$(GOLINT) run

clean: ## テストで生成されたデータファイルやキャッシュを削除します
	rm -f *.data
	$(GOCMD) clean

help: ## ヘルプを表示します
	@echo "使用可能なコマンド:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
