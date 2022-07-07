FROM clickhouse/clickhouse-server:latest
ARG TARGETPLATFORM

RUN wget -qO- https://kopia.io/signing-key | gpg --dearmor -o /usr/share/keyrings/kopia-keyring.gpg && \
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
