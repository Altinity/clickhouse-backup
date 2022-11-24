FROM ${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server}:${CLICKHOUSE_VERSION:-latest} AS builder-base

RUN apt-get update && apt-get install -y gnupg && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 52B59B1571A79DBC054901C0F6BC817356A3D45E && \
    DISTRIB_CODENAME=$(cat /etc/lsb-release | grep DISTRIB_CODENAME | cut -d "=" -f 2) && \
    echo ${DISTRIB_CODENAME} && \
    echo "deb https://ppa.launchpadcontent.net/longsleep/golang-backports/ubuntu ${DISTRIB_CODENAME} main" > /etc/apt/sources.list.d/golang.list && \
    echo "deb-src https://ppa.launchpadcontent.net/longsleep/golang-backports/ubuntu ${DISTRIB_CODENAME} main" >> /etc/apt/sources.list.d/golang.list && \
    apt-get update  && \
    apt-get install -y golang-1.19 make git && \
    mkdir -p /root/go/

RUN ln -nsfv /usr/lib/go-1.19/bin/go /usr/bin/go
VOLUME /root/.cache/go
ENV GOCACHE=/root/.cache/go
ENV GOPATH=/root/go/
ENV GOROOT=/usr/lib/go-1.19/
RUN go env
WORKDIR /src/
# cache modules when go.mod go.sum changed
COPY go.mod go.sum ./
RUN --mount=type=cache,id=clickhouse-backup-gobuild,target=/root/.cache/go go mod download -x

FROM builder-base AS builder-race
ARG TARGETPLATFORM
COPY ./ /src/
RUN mkdir -p ./clickhouse-backup/
RUN --mount=type=cache,id=clickhouse-backup-gobuild,target=/root/.cache/go GOOS=$( echo ${TARGETPLATFORM} | cut -d "/" -f 1) GOARCH=$( echo ${TARGETPLATFORM} | cut -d "/" -f 2) CGO_ENABLED=1 go build --ldflags '-extldflags "-static"' -gcflags "all=-N -l" -race -o ./clickhouse-backup/clickhouse-backup-race ./cmd/clickhouse-backup
RUN ln -nsfv ./clickhouse-backup/clickhouse-backup-race /bin/clickhouse-backup && ldd ./clickhouse-backup/clickhouse-backup-race
COPY entrypoint.sh /entrypoint.sh

FROM builder-base AS builder-docker
COPY ./ /src/
RUN mkdir -p ./build/
RUN --mount=type=cache,id=clickhouse-backup-gobuild,target=/root/.cache/go make build

FROM scratch AS make-build-race
COPY --from=builder-race /src/clickhouse-backup/ /src/clickhouse-backup/
CMD /src/clickhouse-backup/clickhouse-backup-race --help

FROM scratch AS make-build-docker
ARG TARGETPLATFORM
COPY --from=builder-docker /src/build/ /src/build/
CMD /src/build/${TARGETPLATFORM}/clickhouse-backup --help


FROM alpine:3.16 AS image_short
ARG TARGETPLATFORM
MAINTAINER Eugene Klimov <eklimov@altinity.com>

RUN addgroup -S -g 101 clickhouse \
    && adduser -S -h /var/lib/clickhouse -s /bin/bash -G clickhouse -g "ClickHouse server" -u 101 clickhouse

RUN apk update && apk add --no-cache ca-certificates tzdata bash curl && update-ca-certificates

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY build/${TARGETPLATFORM}/clickhouse-backup /bin/clickhouse-backup
RUN chmod +x /bin/clickhouse-backup

# USER clickhouse

ENTRYPOINT ["/entrypoint.sh"]
CMD [ "/bin/clickhouse-backup", "--help" ]


FROM ${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server}:${CLICKHOUSE_VERSION:-latest} AS image_full
ARG TARGETPLATFORM
MAINTAINER Eugene Klimov <eklimov@altinity.com>

RUN apt-get update && apt-get install -y gpg && wget -qO- https://kopia.io/signing-key | gpg --dearmor -o /usr/share/keyrings/kopia-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/kopia-keyring.gpg] http://packages.kopia.io/apt/ stable main" > /etc/apt/sources.list.d/kopia.list && \
    apt-get update -y && \
    apt-get install -y ca-certificates tzdata bash curl restic rsync rclone jq gpg && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/* && rm -rf /var/cache/apt/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY build/${TARGETPLATFORM}/clickhouse-backup /bin/clickhouse-backup
RUN chmod +x /bin/clickhouse-backup

# USER clickhouse

ENTRYPOINT ["/entrypoint.sh"]
CMD [ "/bin/clickhouse-backup", "--help" ]
