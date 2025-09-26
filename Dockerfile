# syntax=docker/dockerfile:1
ARG CLICKHOUSE_VERSION=latest
ARG CLICKHOUSE_IMAGE=clickhouse/clickhouse-server

FROM --platform=$BUILDPLATFORM ${CLICKHOUSE_IMAGE}:${CLICKHOUSE_VERSION} AS builder-base
USER root
# TODO remove ugly workaround for musl, https://www.perplexity.ai/search/2ead4c04-060a-4d78-a75f-f26835238438
RUN rm -fv /etc/apt/sources.list.d/clickhouse.list && \
    find /etc/apt/ -type f -name *.list -exec sed -i 's/ru.archive.ubuntu.com/archive.ubuntu.com/g' {} + && \
    ( apt-get update || true ) && \
    apt-get install -y --no-install-recommends gnupg ca-certificates wget && update-ca-certificates && \
    for srv in "keyserver.ubuntu.com" "pool.sks-keyservers.net" "keys.gnupg.net"; do host $srv; apt-key adv --keyserver $srv --recv-keys 52B59B1571A79DBC054901C0F6BC817356A3D45E; if [ $? -eq 0 ]; then break; fi; done && \
    DISTRIB_CODENAME=$(cat /etc/lsb-release | grep DISTRIB_CODENAME | cut -d "=" -f 2) && \
    echo ${DISTRIB_CODENAME} && \
    echo "deb https://ppa.launchpadcontent.net/longsleep/golang-backports/ubuntu ${DISTRIB_CODENAME} main" > /etc/apt/sources.list.d/golang.list && \
    echo "deb-src https://ppa.launchpadcontent.net/longsleep/golang-backports/ubuntu ${DISTRIB_CODENAME} main" >> /etc/apt/sources.list.d/golang.list && \
    ( apt-get update || true ) && \
    apt-get install -y --no-install-recommends libc-dev golang-1.25 make git gcc musl-dev musl-tools && \
# todo ugly fix for ugly fix, musl.cc is not available from github runner \
    DISTRIB_RELEASE=$(cat /etc/lsb-release | grep DISTRIB_RELEASE | cut -d "=" -f 2) && \
    echo ${DISTRIB_RELEASE} && \
#    wget -nv -O /tmp/megacmd.deb https://mega.nz/linux/repo/xUbuntu_${DISTRIB_RELEASE}/amd64/megacmd-xUbuntu_${DISTRIB_RELEASE}_amd64.deb && \
#    apt install -y "/tmp/megacmd.deb" && \
#    mega-get https://mega.nz/file/zQwVHSYb#8WqqMUCTbbEVKDW55NPrRnM2-4SC-numNCLDKoTWtwQ /root/ && \
    wget -nv -P /root/ https://musl.cc/aarch64-linux-musl-cross.tgz && \
    tar -xvf /root/aarch64-linux-musl-cross.tgz -C /root/ && \
    mkdir -p /root/go/

RUN ln -nsfv /usr/lib/go-1.25/bin/go /usr/bin/go
VOLUME /root/.cache/go
ENV GOCACHE=/root/.cache/go
ENV GOPATH=/root/go/
ENV GOROOT=/usr/lib/go-1.25/
RUN go env
WORKDIR /src/
# cache modules when go.mod go.sum changed
COPY go.mod go.sum ./
RUN --mount=type=cache,id=clickhouse-backup-gobuild,target=/root/ go mod download -x

FROM builder-base AS builder-race
ARG TARGETPLATFORM
COPY ./ /src/
RUN mkdir -p ./clickhouse-backup/
RUN --mount=type=cache,id=clickhouse-backup-gobuild,target=/root/ GOOS=$( echo ${TARGETPLATFORM} | cut -d "/" -f 1) GOARCH=$( echo ${TARGETPLATFORM} | cut -d "/" -f 2) CC=musl-gcc CGO_ENABLED=1 go build -trimpath -cover -buildvcs=false -ldflags "-X 'main.version=race' -linkmode=external -extldflags '-static'" -race -o ./clickhouse-backup/clickhouse-backup-race ./cmd/clickhouse-backup
RUN cp -l ./clickhouse-backup/clickhouse-backup-race /bin/clickhouse-backup && echo "$(ldd ./clickhouse-backup/clickhouse-backup-race 2>&1 || true)" | grep -c "not a dynamic executable"
RUN --mount=type=cache,id=clickhouse-backup-gobuild,target=/root/ GOOS=$( echo ${TARGETPLATFORM} | cut -d "/" -f 1) GOARCH=$( echo ${TARGETPLATFORM} | cut -d "/" -f 2) GOEXPERIMENT=boringcrypto CC=musl-gcc CGO_ENABLED=1 go build -trimpath -cover -buildvcs=false -ldflags "-X 'main.version=race-fips' -linkmode=external -extldflags '-static'" -race -o ./clickhouse-backup/clickhouse-backup-race-fips ./cmd/clickhouse-backup
RUN cp -l ./clickhouse-backup/clickhouse-backup-race-fips /bin/clickhouse-backup-fips && echo "$(ldd ./clickhouse-backup/clickhouse-backup-race-fips 2>&1 || true)" | grep -c "not a dynamic executable"
COPY entrypoint.sh /entrypoint.sh


FROM builder-base AS builder-docker
COPY ./ /src/
RUN mkdir -p ./build/
RUN --mount=type=cache,id=clickhouse-backup-gobuild,target=/root/ make build


FROM builder-base AS builder-fips
COPY ./ /src/
RUN mkdir -p ./build/
RUN --mount=type=cache,id=clickhouse-backup-gobuild,target=/root/ make build-fips


FROM scratch AS make-build-race
COPY --from=builder-race /src/clickhouse-backup/ /src/clickhouse-backup/
CMD /src/clickhouse-backup/clickhouse-backup-race --help


FROM scratch AS make-build-race-fips
COPY --from=builder-race /src/clickhouse-backup/ /src/clickhouse-backup/
CMD /src/clickhouse-backup/clickhouse-backup-race-fips --help


FROM scratch AS make-build-docker
ARG TARGETPLATFORM
COPY --from=builder-docker /src/build/ /src/build/
CMD /src/build/${TARGETPLATFORM}/clickhouse-backup --help


FROM scratch AS make-build-fips
ARG TARGETPLATFORM
COPY --from=builder-fips /src/build/ /src/build/
CMD /src/build/${TARGETPLATFORM}/clickhouse-backup-fips --help


FROM alpine:3.21 AS image_short
ARG TARGETPLATFORM
ARG VERSION=unknown
MAINTAINER Eugene Klimov <eklimov@altinity.com>
LABEL "org.opencontainers.image.version"=${VERSION}
LABEL "org.opencontainers.image.vendor"="Altinity Inc."
LABEL "org.opencontainers.image.licenses"="MIT"
LABEL "org.opencontainers.image.title"="Altinity Backup for ClickHouse"
LABEL "org.opencontainers.image.description"="A tool for easy ClickHouse backup and restore with support for many cloud and non-cloud storage types."
LABEL "org.opencontainers.image.source"="https://github.com/Altinity/clickhouse-backup"
LABEL "org.opencontainers.image.documentation"="https://github.com/Altinity/clickhouse-backup/blob/master/Manual.md"

RUN addgroup -S -g 101 clickhouse \
    && adduser -S -h /var/lib/clickhouse -s /bin/bash -G clickhouse -g "ClickHouse server" -u 101 clickhouse
RUN apk update && apk add --no-cache ca-certificates tzdata bash curl && update-ca-certificates
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY build/${TARGETPLATFORM}/clickhouse-backup /bin/clickhouse-backup
RUN chmod +x /bin/clickhouse-backup
# RUN apk add --no-cache libcap-setcap libcap-getcap && setcap cap_sys_nice=+ep /bin/clickhouse-backup
# USER clickhouse
ENTRYPOINT ["/entrypoint.sh"]
CMD [ "/bin/clickhouse-backup", "--help" ]


FROM image_short AS image_fips
ARG TARGETPLATFORM
ARG VERSION=unknown
LABEL "org.opencontainers.image.version"=${VERSION}
LABEL "org.opencontainers.image.vendor"="Altinity Inc."
LABEL "org.opencontainers.image.licenses"="MIT"
LABEL "org.opencontainers.image.title"="Altinity Backup for ClickHouse"
LABEL "org.opencontainers.image.description"="A tool for easy ClickHouse backup and restore with support for many cloud and non-cloud storage types."
LABEL "org.opencontainers.image.source"="https://github.com/Altinity/clickhouse-backup"
LABEL "org.opencontainers.image.documentation"="https://github.com/Altinity/clickhouse-backup/blob/master/Manual.md"

MAINTAINER Eugene Klimov <eklimov@altinity.com>
COPY build/${TARGETPLATFORM}/clickhouse-backup-fips /bin/clickhouse-backup
RUN chmod +x /bin/clickhouse-backup
# RUN apk add --no-cache libcap-setcap libcap-getcap && setcap cap_sys_nice=+ep /bin/clickhouse-backup


FROM ubuntu:24.04 AS image_full
ARG TARGETPLATFORM
ARG VERSION=unknown
MAINTAINER Eugene Klimov <eklimov@altinity.com>
LABEL "org.opencontainers.image.version"=${VERSION}
LABEL "org.opencontainers.image.vendor"="Altinity Inc."
LABEL "org.opencontainers.image.licenses"="MIT"
LABEL "org.opencontainers.image.title"="Altinity Backup for ClickHouse"
LABEL "org.opencontainers.image.description"="A tool for easy ClickHouse backup and restore with support for many cloud and non-cloud storage types."
LABEL "org.opencontainers.image.source"="https://github.com/Altinity/clickhouse-backup"
LABEL "org.opencontainers.image.documentation"="https://github.com/Altinity/clickhouse-backup/blob/master/Manual.md"

RUN bash -xec "apt-get update && apt-get install --no-install-recommends -y xxd bsdmainutils parallel wget && apt-get install -y gpg curl && wget -qO- https://kopia.io/signing-key | gpg --dearmor --verbose -o /usr/share/keyrings/kopia-keyring.gpg && \
    echo 'deb [signed-by=/usr/share/keyrings/kopia-keyring.gpg] https://packages.kopia.io/apt/ stable main' > /etc/apt/sources.list.d/kopia.list && \
    wget -qO- 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | gpg --dearmor --verbose -o /usr/share/keyrings/clickhouse-keyring.gpg && \
    echo 'deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=$(dpkg --print-architecture)] https://packages.clickhouse.com/deb stable main' > /etc/apt/sources.list.d/clickhouse.list && \
    apt-get update -y && \
    apt-get install --no-install-recommends -y ca-certificates tzdata bash curl restic rsync rclone jq gpg kopia libcap2-bin clickhouse-client && \
    update-ca-certificates && \
    wget -q 'https://github.com/mikefarah/yq/releases/latest/download/yq_linux_$(dpkg --print-architecture)' -c -O /usr/bin/yq && chmod +x /usr/bin/yq && \
    rm -rf /var/lib/apt/lists/* && rm -rf /var/cache/apt/*"

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY build/${TARGETPLATFORM}/clickhouse-backup /bin/clickhouse-backup
RUN chmod +x /bin/clickhouse-backup
# RUN apk add --no-cache libcap-setcap libcap-getcap && setcap cap_sys_nice=+ep /bin/clickhouse-backup
# USER clickhouse
ENTRYPOINT ["/entrypoint.sh"]
CMD [ "/bin/clickhouse-backup", "--help" ]
