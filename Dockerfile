FROM alpine:3.15
ARG TARGETPLATFORM

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
