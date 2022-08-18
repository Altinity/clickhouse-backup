FROM hub.deepin.com/library/golang:bullseye AS builder
WORKDIR /src
ADD . .
RUN go env -w GOPROXY="https://goproxy.cn,direct"
RUN make config


FROM hub.deepin.com/public/alpine:3.16.0-udcp AS runner
ARG TARGETPLATFORM

RUN addgroup -g 101 -S clickhouse && adduser -S -h /var/lib/clickhouse -s /bin/bash -G clickhouse -g "ClickHouse server" -u 101 clickhouse # buildkit
RUN apk update && apk add --no-cache ca-certificates tzdata bash curl && update-ca-certificates # buildkit

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY --from=builder /src/build/${TARGETPLATFORM}/clickhouse-backup /bin/clickhouse-backup
COPY --from=builder /src/clickhouse-backup/config.yml /etc/clickhouse-backup/config.yml
RUN chmod +x /bin/clickhouse-backup

#docker 容器运行，clickhouse数据存储路径利用数据卷挂载的方式，不能单独指定clickhouse用户
#USER clickhouse

ENTRYPOINT ["/entrypoint.sh"]
CMD [ "/bin/clickhouse-backup", "--help" ]
