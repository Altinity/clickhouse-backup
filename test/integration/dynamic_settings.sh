#!/bin/bash
date
if [[ ! -d /hdd1_data ]]; then
  mkdir -pv /hdd1_data
fi

if [[ ! -d /hdd2_data ]]; then
  mkdir -pv /hdd2_data
fi
chown -v clickhouse:clickhouse /hdd1_data /hdd2_data
cat <<EOT > /etc/clickhouse-server/config.d/storage_configuration.xml
<yandex>
  <storage_configuration>
    <disks>
      <hdd1>
         <path>/hdd1_data/</path>
      </hdd1>
      <hdd2>
         <path>/hdd2_data/</path>
      </hdd2>
    </disks>
    <policies>
      <jbod>
        <volumes>
          <jbod_hdd_volume>
            <disk>hdd1</disk>
            <disk>hdd2</disk>
          </jbod_hdd_volume>
        </volumes>
      </jbod>
      <!-- <hot_and_cold>
        <volumes>
          <hot_volume>
            <disk>default</disk>
          </hot_volume>
          <cold_volume>
              <disk>hdd1</disk>
              <disk>hdd2</disk>
          </cold_volume>
        </volumes>
      </hot_and_cold> -->
    </policies>
  </storage_configuration>
</yandex>
EOT

if [[ "${CLICKHOUSE_VERSION}" =~ ^21\.1[0-9] || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[0-9]+ ]]; then

  if [[ ! -d /hdd3_data ]]; then
    mkdir -pv /hdd3_data
  fi
  chown -v clickhouse:clickhouse /hdd3_data

cat <<EOT > /etc/clickhouse-server/config.d/storage_configuration_encrypted.xml
<yandex>
  <storage_configuration>
    <disks>
      <hdd3>
        <type>local</type>
        <path>/hdd3_data/</path>
      </hdd3>
      <hdd3_encrypted>
        <type>encrypted</type>
        <disk>hdd3</disk>
        <path>hdd3_data_encrypted/</path>
        <key>1234567890ABCDFE</key>
      </hdd3_encrypted>
    </disks>
    <policies>
      <hdd3_only_encrypted>
          <volumes>
              <hdd3_only_encrypted>
                  <disk>hdd3_encrypted</disk>
              </hdd3_only_encrypted>
          </volumes>
      </hdd3_only_encrypted>
    </policies>
  </storage_configuration>
</yandex>
EOT

fi

if [[ "${CLICKHOUSE_VERSION}" =~ ^21\.[8-9]|^21\.[0-9]{2} || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[0-9]+ ]]; then

cat <<EOT > /etc/clickhouse-server/config.d/storage_configuration_s3.xml
<yandex>
  <storage_configuration>
    <disks>
      <disk_s3>
        <type>s3</type>
        <endpoint>http://minio:9000/clickhouse/disk_s3/</endpoint>
        <access_key_id>access-key</access_key_id>
        <secret_access_key>it-is-my-super-secret-key</secret_access_key>
        <send_metadata>true</send_metadata>
      </disk_s3>
    </disks>
    <policies>
      <s3_only>
          <volumes>
              <s3_only>
                  <disk>disk_s3</disk>
              </s3_only>
          </volumes>
      </s3_only>
    </policies>
  </storage_configuration>
</yandex>
EOT

fi

if [[ "${CLICKHOUSE_VERSION}" =~ ^21\.12 || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[0-9]+ ]]; then

cat <<EOT > /etc/clickhouse-server/config.d/storage_configuration_encrypted_s3.xml
<yandex>
  <storage_configuration>
    <disks>
      <disk_s3>
        <type>s3</type>
        <endpoint>http://minio:9000/clickhouse/disk_s3/</endpoint>
        <access_key_id>access-key</access_key_id>
        <secret_access_key>it-is-my-super-secret-key</secret_access_key>
        <send_metadata>true</send_metadata>
      </disk_s3>
      <disk_s3_encrypted>
        <type>encrypted</type>
        <disk>disk_s3</disk>
        <path>disk_s3_encrypted/</path>
        <algorithm>AES_128_CTR</algorithm>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1">ffeeddccbbaa99887766554433221100</key_hex>
        <current_key_id>1</current_key_id>
        <send_metadata>true</send_metadata>
      </disk_s3_encrypted>
    </disks>
    <policies>
      <s3_only_encrypted>
          <volumes>
              <s3_only_encrypted>
                  <disk>disk_s3_encrypted</disk>
              </s3_only_encrypted>
          </volumes>
      </s3_only_encrypted>
    </policies>
  </storage_configuration>
</yandex>
EOT
fi

# embedded s3 backup configuration
if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^22\.[6-9] || "${CLICKHOUSE_VERSION}" =~ ^22\.1[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^2[3-9]\.[0-9]+ ]]; then

mkdir -p /var/lib/clickhouse/backups_embedded
chown clickhouse:clickhouse /var/lib/clickhouse/backups_embedded

cat <<EOT > /etc/clickhouse-server/config.d/backup_storage_configuration.xml
<?xml version="1.0"?>
<clickhouse>
    <storage_configuration>
        <disks>
            <backups>
              <send_metadata>true</send_metadata>
              <type>s3</type>
              <endpoint>http://minio:9000/clickhouse/backups/</endpoint>
              <access_key_id>access-key</access_key_id>
              <secret_access_key>it-is-my-super-secret-key</secret_access_key>
              <!-- to avoid unnecessary disk space allocations -->
              <cache_enabled>false</cache_enabled>
            </backups>
        </disks>
    </storage_configuration>
    <backups>
        <allowed_disk>backups</allowed_disk>
        <allowed_path>/var/lib/clickhouse/backups_embedded/</allowed_path>
    </backups>
    <merge_tree>
        <allow_remote_fs_zero_copy_replication>1</allow_remote_fs_zero_copy_replication>
    </merge_tree>
</clickhouse>
EOT

fi

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

if [[ "${CLICKHOUSE_VERSION}" =~ ^20\.1[3-9] ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/allow_experimental_database_materialize_mysql.xml
<yandex>
<profiles><default>
 <allow_experimental_database_materialize_mysql>1</allow_experimental_database_materialize_mysql>
</default></profiles>
</yandex>
EOT

fi

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^2[1-9]\.[1-9] ]]; then
cat <<EOT > /etc/clickhouse-server/users.d/allow_experimental_database_materialized_mysql.xml
<yandex>
<profiles><default>
 <allow_experimental_database_materialized_mysql>1</allow_experimental_database_materialized_mysql>
</default></profiles>
</yandex>
EOT

fi

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[1-9] || "${CLICKHOUSE_VERSION}" =~ ^21\.[8-9] || "${CLICKHOUSE_VERSION}" =~ ^21\.1[0-9] ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/allow_experimental_database_materialized_postgresql.xml
<yandex>
<profiles><default>
 <allow_experimental_database_materialized_postgresql>1</allow_experimental_database_materialized_postgresql>
</default></profiles>
</yandex>
EOT

fi
