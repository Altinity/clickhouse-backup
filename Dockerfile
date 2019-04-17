#FROM scratch
FROM alpine

#RUN adduser -D -g '' clickhouse
RUN apk update && apk add --no-cache ca-certificates && update-ca-certificates

COPY clickhouse-backup/clickhouse-backup /bin/clickhouse-backup
RUN chmod +x /bin/clickhouse-backup

#USER clickhouse

ENTRYPOINT [ "/bin/clickhouse-backup" ]
CMD [ "--help" ]
