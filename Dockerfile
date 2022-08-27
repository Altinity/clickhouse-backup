ARG CLICKHOUSE_IMAGE
ARG CLICKHOUSE_VERSION
ARG TARGETPLATFORM

FROM ${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server}:${CLICKHOUSE_VERSION:-latest} AS builder-base
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 52B59B1571A79DBC054901C0F6BC817356A3D45E && \
    DISTRIB_CODENAME=$(cat /etc/lsb-release | grep DISTRIB_CODENAME | cut -d "=" -f 2) && \
    echo "deb https://ppa.launchpadcontent.net/longsleep/golang-backports/ubuntu ${DISTRIB_CODENAME} main" > /etc/apt/sources.list.d/golang.list && \
    echo "deb-src https://ppa.launchpadcontent.net/longsleep/golang-backports/ubuntu ${DISTRIB_CODENAME} main" >> /etc/apt/sources.list.d/golang.list  && \
    apt-get update  && \
    apt-get purge -y golang* && \
    apt-get install -y golang-1.19 make && \
    mkdir -p /root/go/

RUN ln -nsfv /usr/lib/go-1.19/bin/go /usr/bin/go
VOLUME /root/.cache/go
ENV GOCACHE=/root/.cache/go
ENV GOPATH=/root/go/
ENV GOROOT=/usr/lib/go-1.19/
RUN go env

COPY ./ /src/
WORKDIR /src/

FROM builder-base AS build-race
RUN mkdir -p ./clickhouse-backup/
RUN CGO_ENABLED=1 go build -gcflags "all=-N -l" -race -o ./clickhouse-backup/clickhouse-backup-race ./cmd/clickhouse-backup
RUN ln -nsfv ./clickhouse-backup/clickhouse-backup-race /bin/clickhouse-backup
COPY entrypoint.sh /entrypoint.sh


FROM builder-base AS build-release
RUN make build

FROM ${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server}:${CLICKHOUSE_VERSION:-latest}
MAINTAINER Eugene Klimov <eklimov@altinity.com>

RUN wget -qO- https://kopia.io/signing-key | gpg --dearmor -o /usr/share/keyrings/kopia-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/kopia-keyring.gpg] http://packages.kopia.io/apt/ stable main" > /etc/apt/sources.list.d/kopia.list && \
    apt-get update -y && \
    apt-get install -y ca-certificates tzdata bash curl restic rsync rclone jq gpg && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/* && rm -rf /var/cache/apt/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY --from=build-release /build/${TARGETPLATFORM}/clickhouse-backup /bin/clickhouse-backup
RUN chmod +x /bin/clickhouse-backup

# USER clickhouse

ENTRYPOINT ["/entrypoint.sh"]
CMD [ "/bin/clickhouse-backup", "--help" ]
