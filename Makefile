# Origin: https://github.com/innogames/graphite-ch-optimizer/blob/master/Makefile
# MIT License
NAME = clickhouse-backup
VERSION = $(shell git describe --always --tags --abbrev=0 2>/dev/null | sed 's/^v//;s/\([^-]*-g\)/c\1/;s/-/./g')
GIT_COMMIT = $(shell git rev-parse HEAD)
DATE = $(shell date +%F)
VENDOR = "Eugene Klimov <eklimov@altinity.com>"
URL = https://github.com/Altinity/$(NAME)
define DESC =
'Tool for easy ClickHouse backup and restore with S3 and GCS support
 Easy creating and restoring backups of all or specific tables
 Efficient storing of multiple backups on the file system
 Most efficient AWS S3/GCS uploading and downloading with streaming compression
 Support of incremental backups on remote storages'
endef
HOST_OS = $(shell bash -c 'source <(go env) && echo $$GOHOSTOS')
HOST_ARCH = $(shell bash -c 'source <(go env) && echo $$GOHOSTARCH')
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    LDFLAGS += -linkmode=external -extldflags '-static'
endif
GO_BUILD = go build -trimpath -buildvcs=false -tags netgo,osusergo -ldflags "-X 'main.version=$(VERSION)' -X 'main.gitCommit=$(GIT_COMMIT)' -X 'main.buildDate=$(DATE)' -X 'main.buildArch=$(HOST_OS)/$(HOST_ARCH)'"
GO_BUILD_STATIC = go build -trimpath -buildvcs=false -tags netgo,osusergo -ldflags "-X 'main.version=$(VERSION)' -X 'main.gitCommit=$(GIT_COMMIT)' -X 'main.buildDate=$(DATE)' -X 'main.buildArch=$(HOST_OS)/$(HOST_ARCH)' $(LDFLAGS)"
PKG_FILES = build/$(NAME)_$(VERSION).amd64.deb build/$(NAME)_$(VERSION).arm64.deb build/$(NAME)-$(VERSION)-1.amd64.rpm build/$(NAME)-$(VERSION)-1.arm64.rpm

.PHONY: clean all version test

all: build build-fips config packages

version:
	@echo $(VERSION)

clean:
	rm -rf build
	rm -rf $(NAME)

rebuild: clean all

test:
	go vet ./...
	mkdir -v ./_coverage_
	GOCOVERDIR=./_coverage_/ go test -coverprofile=./_coverage_/coverage.out -covermode=atomic -v ./...

build: build/linux/amd64/$(NAME) build/linux/arm64/$(NAME) build/darwin/amd64/$(NAME) build/darwin/arm64/$(NAME)

build/linux/amd64/$(NAME) build/darwin/amd64/$(NAME): GOARCH = amd64
build/linux/arm64/$(NAME) build/darwin/arm64/$(NAME): GOARCH = arm64
build/linux/amd64/$(NAME) build/linux/arm64/$(NAME): GOOS = linux
build/darwin/amd64/$(NAME) build/darwin/arm64/$(NAME): GOOS = darwin
build/linux/amd64/$(NAME) build/linux/arm64/$(NAME) build/darwin/amd64/$(NAME) build/darwin/arm64/$(NAME):
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO_BUILD) -o $@ ./cmd/$(NAME)

build-fips: build/linux/amd64/$(NAME)-fips build/linux/arm64/$(NAME)-fips

build-fips-darwin: build/darwin/amd64/$(NAME)-fips build/darwin/arm64/$(NAME)-fips

build/linux/amd64/$(NAME)-fips build/darwin/amd64/$(NAME)-fips: GOARCH = amd64
build/linux/arm64/$(NAME)-fips build/darwin/arm64/$(NAME)-fips: GOARCH = arm64
build/linux/amd64/$(NAME)-fips build/linux/arm64/$(NAME)-fips: GOOS = linux
build/darwin/amd64/$(NAME)-fips build/darwin/arm64/$(NAME)-fips: GOOS = darwin

build/linux/amd64/$(NAME)-fips build/darwin/amd64/$(NAME)-fips build/linux/arm64/$(NAME)-fips build/darwin/arm64/$(NAME)-fips:
	GOFIPS140=v1.0.0 CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO_BUILD) -o $@ ./cmd/$(NAME) && \
	go tool nm $@ > /tmp/$(NAME)-fips-tags.txt && \
	grep 'fips140' /tmp/$(NAME)-fips-tags.txt 1> /dev/null && \
	rm -fv /tmp/$(NAME)-fips-tags.txt

config: $(NAME)/config.yml

$(NAME)/$(NAME): build/$(HOST_OS)/$(HOST_ARCH)/$(NAME)
	mkdir -p $(NAME)

$(NAME)/config.yml: $(NAME)/$(NAME) build/$(HOST_OS)/$(HOST_ARCH)/$(NAME)
	./build/$(HOST_OS)/$(HOST_ARCH)/$(NAME) default-config > $@

build/linux/amd64/config.yml build/linux/arm64/config.yml build/darwin/amd64/config.yml build/darwin/arm64/config.yml: config
	cp -lv ./$(NAME)/config.yml $@

packages: $(PKG_FILES)

build/linux/amd64/pkg: ARCH = amd64
build/linux/arm64/pkg: ARCH = arm64
build/linux/amd64/pkg build/linux/arm64/pkg: build config
	cd ./build/linux/$(ARCH) && \
	mkdir -pv pkg/etc/$(NAME) && \
	mkdir -pv pkg/usr/bin && \
	cp -lv $(NAME) pkg/usr/bin/ && \
	ln -sfv $(NAME) pkg/usr/bin/$(NAME)-acvp && \
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

$(NAME)/$(NAME)-race:
	CGO_ENABLED=1 $(GO_BUILD_STATIC) -cover -gcflags "all=-N -l" -race -o $@ ./cmd/$(NAME)

build-race-fips: $(NAME)/$(NAME)-race-fips

$(NAME)/$(NAME)-race-fips:
	GOFIPS140=v1.0.0 CGO_ENABLED=1 $(GO_BUILD_STATIC) -cover -gcflags "all=-N -l" -race -o $@ ./cmd/$(NAME)


# run `docker buildx create --use` first time
build-race-docker:
	bash -xce 'docker buildx build --build-arg CLICKHOUSE_VERSION=$${CLICKHOUSE_VERSION:-latest} --build-arg CLICKHOUSE_IMAGE=$${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server} --build-arg VERSION=$(VERSION) \
			--tag $(NAME):build-race --target make-build-race --progress plain --load . && \
		mkdir -pv ./$(NAME) && \
		DOCKER_ID=$$(docker create $(NAME):build-race) && \
		docker cp $${DOCKER_ID}:/src/$(NAME)/$(NAME)-race ./$(NAME)/ && \
		docker rm -f "$${DOCKER_ID}" && \
		cp -fl ./$(NAME)/$(NAME)-race ./$(NAME)/$(NAME)-race-docker'

build-race-fips-docker:
	bash -xce 'docker buildx build --build-arg CLICKHOUSE_VERSION=$${CLICKHOUSE_VERSION:-latest} --build-arg CLICKHOUSE_IMAGE=$${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server} --build-arg VERSION=$(VERSION) \
			--tag $(NAME):build-race-fips --target make-build-race-fips --progress plain --load . && \
		mkdir -pv ./$(NAME) && \
		DOCKER_ID=$$(docker create $(NAME):build-race-fips) && \
		docker cp $${DOCKER_ID}:/src/$(NAME)/$(NAME)-race-fips ./$(NAME)/ && \
		docker rm -f "$${DOCKER_ID}" && \
		cp -fl ./$(NAME)/$(NAME)-race-fips ./$(NAME)/$(NAME)-race-fips-docker'

build-docker:
	bash -xce 'docker buildx build --build-arg CLICKHOUSE_VERSION=$${CLICKHOUSE_VERSION:-latest} --build-arg CLICKHOUSE_IMAGE=$${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server} --build-arg VERSION=$(VERSION) \
			--tag $(NAME):build-docker --target make-build-docker --progress plain --load . && \
		mkdir -pv ./build && \
		DOCKER_ID=$$(docker create $(NAME):build-docker) && \
		docker cp $${DOCKER_ID}:/src/build/. ./build/ && \
		docker rm -f "$${DOCKER_ID}"'

build-fips-docker:
	bash -xce 'docker buildx build --build-arg CLICKHOUSE_VERSION=$${CLICKHOUSE_VERSION:-latest} --build-arg CLICKHOUSE_IMAGE=$${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server} --build-arg VERSION=$(VERSION) \
			--tag $(NAME):build-docker-fips --target make-build-fips --progress plain --load . && \
		mkdir -pv ./build && \
		DOCKER_ID=$$(docker create $(NAME):build-docker-fips) && \
		docker cp $${DOCKER_ID}:/src/build/. ./build/ && \
		docker rm -f "$${DOCKER_ID}"'
