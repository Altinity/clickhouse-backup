#!/usr/bin/env bash
export CLICKHOUSE_VERSION=${CLICKHOUSE_VERSION:-22.8}

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^22\.[7-9]|^22\.[0-9]{2}|^2[3-9]\. ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/allow_deprecated_database_ordinary.xml
<yandex>
<profiles><default>
 <allow_deprecated_database_ordinary>1</allow_deprecated_database_ordinary>
 <allow_deprecated_syntax_for_merge_tree>1</allow_deprecated_syntax_for_merge_tree>
</default></profiles>
</yandex>
EOT

fi

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^22\.[3-9]|^2[3-9]\. ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/allow_experimental_database_materialized_postgresql.xml
<yandex>
<profiles><default>
 <allow_experimental_database_materialized_postgresql>1</allow_experimental_database_materialized_postgresql>
</default></profiles>
</yandex>
EOT

cat <<EOT > /etc/clickhouse-server/users.d/allow_experimental_database_materialized_mysql.xml
<yandex>
<profiles><default>
 <allow_experimental_database_materialized_mysql>1</allow_experimental_database_materialized_mysql>
</default></profiles>
</yandex>
EOT

fi