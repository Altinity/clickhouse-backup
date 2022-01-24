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
PKG_FILES = build/$(NAME)_$(VERSION).amd64.deb build/$(NAME)_$(VERSION).arm64.deb build/$(NAME)-$(VERSION)-1.amd64.rpm build/$(NAME)-$(VERSION)-1.arm64.rpm
HOST_OS = $(shell bash -c 'source <(go env) && echo $$GOHOSTOS')
HOST_ARCH = $(shell bash -c 'source <(go env) && echo $$GOHOSTARCH')

.PHONY: clean all version test

all: build config packages

version:
	@echo $(VERSION)

clean:
	rm -rf build
	rm -rf $(NAME)

rebuild: clean all

test:
	go vet ./...
	go test -v ./...

build: build/linux/amd64/$(NAME) build/linux/arm64/$(NAME) build/darwin/amd64/$(NAME) build/darwin/arm64/$(NAME)

build/linux/amd64/$(NAME) build/darwin/amd64/$(NAME): GOARCH = amd64
build/linux/arm64/$(NAME) build/darwin/arm64/$(NAME): GOARCH = arm64
build/linux/amd64/$(NAME) build/linux/arm64/$(NAME): GOOS = linux
build/darwin/amd64/$(NAME) build/darwin/arm64/$(NAME): GOOS = darwin
build/linux/amd64/$(NAME) build/linux/arm64/$(NAME) build/darwin/amd64/$(NAME) build/darwin/arm64/$(NAME): $(GO_FILES)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO_BUILD) -o $@ ./cmd/$(NAME)

config: $(NAME)/config.yml

$(NAME)/$(NAME): build/$(HOST_OS)/amd64/$(NAME)
	mkdir -p $(NAME)
	cp -lv ./build/linux/amd64/$(NAME) ./$(NAME)/$(NAME)

$(NAME)/config.yml: $(NAME)/$(NAME) build/$(HOST_OS)/$(HOST_ARCH)/$(NAME)
	./build/$(HOST_OS)/$(HOST_ARCH)/$(NAME) default-config > $@

build/linux/amd64/config.yml build/linux/arm64/config.yml build/darwin/amd64/config.yml build/darwin/arm64/config.yml: config
	cp -lv ./$(NAME)/config.yml $@

packages: $(PKG_FILES)

build/linux/amd64/pkg: ARCH = amd64
build/linux/arm64/pkg: ARCH = arm64
.ONESHELL:
build/linux/amd64/pkg build/linux/arm64/pkg: build config
	cd ./build/linux/$(ARCH)
	mkdir -pv pkg/etc/$(NAME)
	mkdir -pv pkg/usr/bin
	cp -lv $(NAME) pkg/usr/bin/
	cp -lv ../../../$(NAME)/config.yml pkg/etc/$(NAME)/config.yml.example


deb: $(word 1, $(PKG_FILES))

deb_arm: $(word 2, $(PKG_FILES))

rpm: $(word 3, $(PKG_FILES))

rpm_arm: $(word 4, $(PKG_FILES))

# Set TYPE to package suffix w/o dot
$(PKG_FILES): PKG_TYPE = $(subst .,,$(suffix $@))
# Set ARCH to package python split('.')[-1].split('_')[-]
$(PKG_FILES): PKG_ARCH = $(word $(shell echo $(words $(subst ., ,$@))-1 | bc),$(subst ., ,$@))
$(PKG_FILES): build/linux/amd64/pkg build/linux/arm64/pkg
	fpm --verbose \
		-s dir \
		-a $(PKG_ARCH) \
		-t $(PKG_TYPE) \
		--vendor $(VENDOR) \
		-m $(VENDOR) \
		--url $(URL) \
		--description $(DESC) \
		--license MIT \
		-n $(NAME) \
		-v $(VERSION) \
		-p build/linux/$(PKG_ARCH) \
		build/linux/$(PKG_ARCH)/pkg/=/

build-race: $(NAME)/$(NAME)-race

$(NAME)/$(NAME)-race: $(GO_FILES)
	CGO_ENABLED=1 $(GO_BUILD) -gcflags "all=-N -l" -race -o $@ ./cmd/$(NAME)
