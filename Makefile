# Origin: https://github.com/innogames/graphite-ch-optimizer/blob/master/Makefile
# MIT License
NAME = clickhouse-backup
VERSION = $(shell git describe --always --tags --abbrev=0 2>/dev/null | sed 's/^v//;s/\([^-]*-g\)/c\1/;s/-/./g')
GIT_COMMIT = $(shell git rev-parse HEAD)
DATE = $(shell date +%F)
VENDOR = "Alexander Akulov <alexakulov86@gmail.com>"
URL = https://github.com/AlexAkulov/$(NAME)
define DESC =
'Tool for easy ClickHouse backup and restore with S3 and GCS support
 Easy creating and restoring backups of all or specific tables
 Efficient storing of multiple backups on the file system
 Most efficient AWS S3/GCS uploading and downloading with streaming compression
 Support of incremental backups on remote storages'
endef
GO_FILES = $(shell find -name '*.go')
GO_BUILD = go build -ldflags "-X 'main.version=$(VERSION)' -X 'main.gitCommit=$(GIT_COMMIT)' -X 'main.buildDate=$(DATE)'"
PKG_FILES = build/$(NAME)_$(VERSION)_amd64.deb build/$(NAME)-$(VERSION)-1.x86_64.rpm
export CGO_ENABLED = 0

.PHONY: clean all version test

all: build config

version:
	@echo $(VERSION)

clean:
	rm -rf build
	rm -rf $(NAME)

rebuild: clean all

test:
	go vet ./...
	go test -v ./...

build: $(NAME)/$(NAME)

config: $(NAME)/config.yml

$(NAME)/config.yml: $(NAME)/$(NAME)
	./$(NAME)/$(NAME) default-config > $@

$(NAME)/$(NAME): $(GO_FILES)
	$(GO_BUILD) -o $@ .

build/$(NAME): $(GO_FILES)
	GOOS=linux GOARCH=amd64 $(GO_BUILD) -o $@ .

packages: $(PKG_FILES)

.ONESHELL:
build/pkg: build/$(NAME) $(NAME)/config.yml
	cd build
	mkdir -p pkg/etc/$(NAME)
	mkdir -p pkg/usr/bin
	cp -l $(NAME) pkg/usr/bin/
	cp -l ../$(NAME)/config.yml pkg/etc/$(NAME)/config.yml.example

deb: $(word 1, $(PKG_FILES))

rpm: $(word 2, $(PKG_FILES))

# Set TYPE to package suffix w/o dot
$(PKG_FILES): TYPE = $(subst .,,$(suffix $@))
$(PKG_FILES): build/pkg
	fpm --verbose \
		-s dir \
		-a x86_64 \
		-t $(TYPE) \
		--vendor $(VENDOR) \
		-m $(VENDOR) \
		--url $(URL) \
		--description $(DESC) \
		--license MIT \
		-n $(NAME) \
		-v $(VERSION) \
		-p build \
		build/pkg/=/
