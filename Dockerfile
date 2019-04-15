# Based on andrii29 comment in https://github.com/AlexAkulov/clickhouse-backup/issues/10 
# devinotelecom/clickhouse-backup on hub.docker.com
FROM golang:1.12.1-stretch AS builder

RUN git clone https://github.com/AlexAkulov/clickhouse-backup.git /go/src/clickhouse-backup
WORKDIR /go/src/clickhouse-backup
RUN go get
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /go/bin/clickhouse-backup

FROM scratch

COPY --from=builder /go/bin/clickhouse-backup /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/clickhouse-backup"]
