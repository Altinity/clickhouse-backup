FROM alpine:3.12

RUN addgroup -S -g 101 clickhouse \
    && adduser -S -h /var/lib/clickhouse -s /bin/bash -G clickhouse -g "ClickHouse server" -u 101 clickhouse \
    && mkdir -p /var/lib/clickhouse/backup /etc/clickhouse-backup \
    && chown -R clickhouse:clickhouse /var/lib/clickhouse \
    && chmod ugo+Xrw -R /var/lib/clickhouse /etc/clickhouse-backup

RUN apk update && apk add --no-cache ca-certificates tzdata bash curl && update-ca-certificates

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
COPY clickhouse-backup/clickhouse-backup /bin/clickhouse-backup
RUN chmod +x /bin/clickhouse-backup

USER clickhouse

ENTRYPOINT ["/entrypoint.sh"]
CMD [ "clickhouse-backup", "--help" ]
