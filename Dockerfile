FROM alpine AS build
ARG TRAVIS_TAG
ARG TRAVIS_COMMIT
RUN apk update && apk add --no-cache ca-certificates go git musl-dev tzdata && update-ca-certificates
COPY . /build/
WORKDIR /build/
ENV CGO_ENABLED=0
RUN go mod vendor
RUN go build -ldflags "-w -s -X main.version=${TRAVIS_TAG} -X main.gitCommit=${TRAVIS_COMMIT} -X main.buildDate=$(date +%Y-%m-%d)" -o ./clickhouse-backup

FROM alpine
RUN apk update && apk add --no-cache ca-certificates tzdata && update-ca-certificates
COPY --from=build /build/clickhouse-backup /bin/clickhouse-backup
ENTRYPOINT [ "/bin/clickhouse-backup" ]
CMD [ "--help" ]
