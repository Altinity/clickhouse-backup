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
      <hdd1_only>
        <volumes>
          <hdd1_only_volume>
            <disk>hdd1</disk>
          </hdd1_only_volume>
        </volumes>
      </hdd1_only>
      <jbod>
        <volumes>
          <jbod_hdd_volume>
            <disk>hdd1</disk>
            <disk>hdd2</disk>
          </jbod_hdd_volume>
        </volumes>
      </jbod>
      <hot_and_cold>
        <volumes>
          <hot_volume>
            <disk>default</disk>
          </hot_volume>
          <cold_volume>
              <disk>hdd1</disk>
              <disk>hdd2</disk>
          </cold_volume>
        </volumes>
      </hot_and_cold>
    </policies>
  </storage_configuration>
</yandex>
EOT

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^21\.1[0-9] || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then

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

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^21\.[8-9]|^21\.[0-9]{2} || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then
if [[ -f /var/lib/clickhouse/storage_configuration_s3.xml ]]; then
  cp -fv /var/lib/clickhouse/storage_configuration_s3.xml /etc/clickhouse-server/config.d/storage_configuration_s3.xml
else
  S3_DISK_TYPE="<type>s3</type>"
  if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^2[5-9]\.[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then
    S3_DISK_TYPE="<type>object_storage</type><object_storage_type>s3</object_storage_type><metadata_type>local</metadata_type>"
  fi

cat <<EOT > /etc/clickhouse-server/config.d/storage_configuration_s3.xml
<yandex>
  <storage_configuration>
    <disks>
      <disk_s3>
        ${S3_DISK_TYPE}
        <endpoint>https://minio:9000/clickhouse/disk_s3/{cluster}/{shard}/</endpoint>
        <!-- https://github.com/Altinity/clickhouse-backup/issues/691
        <access_key_id>access_key</access_key_id>
        <secret_access_key>it_is_my_super_secret_key</secret_access_key>
        -->
        <use_environment_credentials>1</use_environment_credentials>
        <!-- to avoid slow startup -->
        <send_metadata>false</send_metadata>
        <!-- https://github.com/Altinity/clickhouse-backup/issues/859#issuecomment-2896880448, https://github.com/ClickHouse/ClickHouse/issues/80647 -->
        <!-- <metadata_path>/var/lib/clickhouse/disks/disk_s3</metadata_path> -->
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
fi

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^22\.[6-9]+ || "${CLICKHOUSE_VERSION}" =~ ^22\.1[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^2[3-9]\.[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then

if [[ "" != "${QA_GCS_OVER_S3_BUCKET}" ]]; then
if [[ -f /var/lib/clickhouse/storage_configuration_gcs.xml ]]; then
  cp -fv /var/lib/clickhouse/storage_configuration_gcs.xml /etc/clickhouse-server/config.d/storage_configuration_gcs.xml
else

cat <<EOT > /etc/clickhouse-server/config.d/storage_configuration_gcs.xml
<yandex>
  <storage_configuration>
    <disks>
      <disk_gcs>
        <type>s3</type>
        <endpoint>https://storage.googleapis.com/${QA_GCS_OVER_S3_BUCKET}/clickhouse_backup_disk_gcs_over_s3/${HOSTNAME}/{cluster}/{shard}/</endpoint>
        <access_key_id>${QA_GCS_OVER_S3_ACCESS_KEY}</access_key_id>
        <secret_access_key>${QA_GCS_OVER_S3_SECRET_KEY}</secret_access_key>
        <!-- to avoid slow startup -->
        <send_metadata>false</send_metadata>
        <support_batch_delete>false</support_batch_delete>
      </disk_gcs>
    </disks>
    <policies>
      <gcs_only>
          <volumes>
              <gcs_only>
                  <disk>disk_gcs</disk>
              </gcs_only>
          </volumes>
      </gcs_only>
    </policies>
  </storage_configuration>
</yandex>
EOT

fi
fi

fi

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^21\.12 || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then

if [[ -f /var/lib/clickhouse/storage_configuration_encrypted_s3.xml ]]; then
  cp -fv /var/lib/clickhouse/storage_configuration_encrypted_s3.xml /etc/clickhouse-server/config.d/storage_configuration_encrypted_s3.xml
else

cat <<EOT > /etc/clickhouse-server/config.d/storage_configuration_encrypted_s3.xml
<yandex>
  <storage_configuration>
    <disks>
      <disk_s3>
        <type>s3</type>
        <endpoint>https://minio:9000/clickhouse/disk_s3/</endpoint>
        <!-- https://github.com/Altinity/clickhouse-backup/issues/691
        <access_key_id>access_key</access_key_id>
        <secret_access_key>it_is_my_super_secret_key</secret_access_key>
        -->
        <use_environment_credentials>1</use_environment_credentials>
        <!-- to avoid slow startup -->
        <send_metadata>false</send_metadata>
      </disk_s3>
      <disk_s3_encrypted>
        <type>encrypted</type>
        <disk>disk_s3</disk>
        <path>disk_s3_encrypted/</path>
        <algorithm>AES_128_CTR</algorithm>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1">ffeeddccbbaa99887766554433221100</key_hex>
        <current_key_id>1</current_key_id>
        <!-- to avoid slow startup -->
        <send_metadata>false</send_metadata>
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
fi

# embedded local backup configuration
if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^22\.[6-9] || "${CLICKHOUSE_VERSION}" =~ ^22\.1[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^2[3-9]\.[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then

mkdir -p /var/lib/clickhouse/disks/backups_local/ /var/lib/clickhouse/backups_embedded/
chown -R clickhouse /var/lib/clickhouse/disks/ /var/lib/clickhouse/backups_embedded/

cat <<EOT > /etc/clickhouse-server/config.d/backup_storage_configuration_local.xml
<?xml version="1.0"?>
<clickhouse>
  <storage_configuration>
    <disks>
      <backups_local>
        <type>local</type>
        <path>/var/lib/clickhouse/disks/backups_local/</path>
      </backups_local>
    </disks>
  </storage_configuration>
  <backups>
    <allowed_disk>backups_local</allowed_disk>
    <allowed_path>/var/lib/clickhouse/backups_embedded/</allowed_path>
  </backups>
</clickhouse>
EOT

fi

# embedded s3 backup configuration
if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^22\.[6-9] || "${CLICKHOUSE_VERSION}" =~ ^22\.1[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^2[3-9]\.[0-9]+ ]]; then

mkdir -p /var/lib/clickhouse/disks/backups_s3/ /var/lib/clickhouse/backups_embedded/
chown -R clickhouse /var/lib/clickhouse/disks/ /var/lib/clickhouse/backups_embedded/

cat <<EOT > /etc/clickhouse-server/config.d/backup_storage_configuration_s3.xml
<?xml version="1.0"?>
<clickhouse>
  <storage_configuration>
    <disks>
      <backups_s3>
        <type>s3</type>
        <endpoint>https://minio:9000/clickhouse/backups_s3/{cluster}/{shard}/</endpoint>
        <!-- https://github.com/Altinity/clickhouse-backup/issues/691
        <access_key_id>access_key</access_key_id>
        <secret_access_key>it_is_my_super_secret_key</secret_access_key>
        -->
        <use_environment_credentials>1</use_environment_credentials>
        <cache_enabled>false</cache_enabled>
        <!-- to avoid slow startup -->
        <send_metadata>false</send_metadata>
      </backups_s3>
    </disks>
  </storage_configuration>
  <backups>
    <allowed_disk>backups_local</allowed_disk>
    <allowed_disk>backups_s3</allowed_disk>
    <allowed_path>/var/lib/clickhouse/backups_embedded/</allowed_path>
  </backups>
</clickhouse>
EOT

# zero replication is buggy,  can't freeze table: code: 344, message: FREEZE PARTITION queries are disabled.
# https://github.com/ClickHouse/ClickHouse/issues/62167#issuecomment-2031774983
#cat <<EOT > /etc/clickhouse-server/config.d/zero_copy_replication.xml
#<yandex>
#  <merge_tree>
#    <allow_remote_fs_zero_copy_replication>1</allow_remote_fs_zero_copy_replication>
#    <disable_freeze_partition_for_zero_copy_replication>0</disable_freeze_partition_for_zero_copy_replication>
#  </merge_tree>
#</yandex>
#EOT

cat <<EOT > /etc/clickhouse-server/config.d/zookeeper_log.xml
<yandex>
  <zookeeper_log>
        <database>system</database>
        <table>zookeeper_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <flush_on_crash>true</flush_on_crash>
    </zookeeper_log>
</yandex>
EOT
fi

# s3_plain and azure backup configuration
if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^23\.3 || "${CLICKHOUSE_VERSION}" =~ ^23\.[4-9] || "${CLICKHOUSE_VERSION}" =~ ^23\.1[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^2[4-9]\.[0-9]+ ]]; then

mkdir -p /var/lib/clickhouse/disks/backups_s3_plain/
chown -R clickhouse /var/lib/clickhouse/disks/

cat <<EOT > /etc/clickhouse-server/config.d/backup_storage_configuration_s3_plain.xml
<?xml version="1.0"?>
<clickhouse>
  <storage_configuration>
    <disks>
      <backups_s3_plain>
        <type>s3_plain</type>
        <endpoint>https://minio:9000/clickhouse/backups_s3_plain/{cluster}/{shard}/</endpoint>
        <!-- https://github.com/Altinity/clickhouse-backup/issues/691
        <access_key_id>access_key</access_key_id>
        <secret_access_key>it_is_my_super_secret_key</secret_access_key>
        -->
        <use_environment_credentials>1</use_environment_credentials>
        <cache_enabled>false</cache_enabled>
      </backups_s3_plain>
    </disks>
  </storage_configuration>
  <backups>
    <allowed_disk>backups_local</allowed_disk>
    <allowed_disk>backups_s3</allowed_disk>
    <allowed_disk>backups_s3_plain</allowed_disk>
  </backups>
</clickhouse>
EOT

mkdir -p /var/lib/clickhouse/disks/backups_azure/
chown -R clickhouse /var/lib/clickhouse/disks/

if [[ -f /var/lib/clickhouse/storage_configuration_azblob.xml ]]; then
  cp -fv /var/lib/clickhouse/storage_configuration_azblob.xml /etc/clickhouse-server/config.d/backup_storage_configuration_azblob.xml
else

cat <<EOT > /etc/clickhouse-server/config.d/storage_configuration_azblob.xml
<?xml version="1.0"?>
<clickhouse>
  <storage_configuration>
    <disks>
      <disk_azblob>
        <type>azure_blob_storage</type>
        <storage_account_url>http://azure:10000/devstoreaccount1</storage_account_url>
        <container_name>azure-disk</container_name>
        <!--  https://github.com/Azure/Azurite/blob/main/README.md#usage-with-azure-storage-sdks-or-tools -->
        <account_name>devstoreaccount1</account_name>
        <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
        <cache_enabled>false</cache_enabled>
        <use_native_copy>true</use_native_copy>
      </disk_azblob>
      <backups_azure>
        <type>azure_blob_storage</type>
        <storage_account_url>http://azure:10000/devstoreaccount1</storage_account_url>
        <container_name>azure-backup-disk</container_name>
        <!--  https://github.com/Azure/Azurite/blob/main/README.md#usage-with-azure-storage-sdks-or-tools -->
        <account_name>devstoreaccount1</account_name>
        <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
        <cache_enabled>false</cache_enabled>
        <use_native_copy>true</use_native_copy>
      </backups_azure>
    </disks>
    <policies>
      <azure_only>
        <volumes>
          <azure_only>
            <disk>disk_azblob</disk>
          </azure_only>
        </volumes>
      </azure_only>
    </policies>
  </storage_configuration>
  <backups>
      <allowed_disk>backups_local</allowed_disk>
      <allowed_disk>backups_s3</allowed_disk>
      <allowed_disk>backups_s3_plain</allowed_disk>
      <allowed_disk>backups_azure</allowed_disk>
  </backups>
</clickhouse>
EOT
fi

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

if [[ "${CLICKHOUSE_VERSION}" =~ ^20\.1[3-9] || "${CLICKHOUSE_VERSION}" =~ ^21\.[1-8]$ ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/allow_experimental_database_materialize_mysql.xml
<yandex>
<profiles><default>
 <allow_experimental_database_materialize_mysql>1</allow_experimental_database_materialize_mysql>
</default></profiles>
</yandex>
EOT

fi

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^21\.9 || "${CLICKHOUSE_VERSION}" =~ ^21\.1[012] || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[1-9] || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then
cat <<EOT > /etc/clickhouse-server/users.d/allow_experimental_database_materialized_mysql.xml
<yandex>
<profiles><default>
 <allow_experimental_database_materialized_mysql>1</allow_experimental_database_materialized_mysql>
</default></profiles>
</yandex>
EOT

fi

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^21\.[8-9] || "${CLICKHOUSE_VERSION}" =~ ^21\.1[0-9] || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[1-9] || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/allow_experimental_database_materialized_postgresql.xml
<yandex>
<profiles><default>
 <allow_experimental_database_materialized_postgresql>1</allow_experimental_database_materialized_postgresql>
</default></profiles>
</yandex>
EOT

fi

# https://github.com/Altinity/clickhouse-backup/issues/1058, clickhouse-keeper-client available from 23.9
if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^23\.9 || "${CLICKHOUSE_VERSION}" =~ ^23\.1[0-9] || "${CLICKHOUSE_VERSION}" =~ ^2[4-9]\.[1-9] ]]; then

clickhouse-keeper-client -p 2181 -h zookeeper -q "touch '/custom_zookeeper_root'"

cat <<EOT > /etc/clickhouse-server/config.d/custom_zookeeper_root.xml
<yandex>
  <zookeeper>
    <root>/custom_zookeeper_root</root>
  </zookeeper>
</yandex>
EOT

fi

# zookeeper RBAC available from 21.9
if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^21\.9 || "${CLICKHOUSE_VERSION}" =~ ^21\.1[0-9] || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[1-9] || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then

mkdir -p /var/lib/clickhouse/access
chown clickhouse:clickhouse /var/lib/clickhouse/access

cat <<EOT > /etc/clickhouse-server/config.d/replicated_user_directories.xml
<yandex>
  <user_directories replace="replace">
    <users_xml>
      <path>users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access</zookeeper_path>
    </replicated>
  </user_directories>
</yandex>
EOT

fi

# @todo LIVE VIEW deprecated, available 21.3+
if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^21\.[3-9] || "${CLICKHOUSE_VERSION}" =~ ^21\.1[0-9] || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[1-9] || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/allow_experimental_live_view.xml
<yandex>
<profiles><default>
 <allow_experimental_live_view>1</allow_experimental_live_view>
</default></profiles>
</yandex>
EOT

fi

# WINDOW VIEW available 21.12+
if [[ "${CLICKHOUSE_VERSION}" == "head"  || "${CLICKHOUSE_VERSION}" =~ ^21\.12 || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[1-9] || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/allow_experimental_window_view.xml
<yandex>
<profiles><default>
 <allow_experimental_window_view>1</allow_experimental_window_view>
</default></profiles>
</yandex>
EOT

fi

# blob_storage_log available in 23.11
if [[ "$CLICKHOUSE_VERSION" == "head" || "${CLICKHOUSE_VERSION}" =~ ^23\.1[1-9] || "${CLICKHOUSE_VERSION}" =~ ^2[4-9]\.[1-9] ]]; then

cat <<EOT > /etc/clickhouse-server/config.d/blob_storage_log.xml
<yandex>
   <blob_storage_log replace="1">
        <database>system</database>
        <table>blob_storage_log</table>
        <engine>ENGINE = MergeTree PARTITION BY (event_date)
                ORDER BY (event_time)
                TTL event_date + INTERVAL 1 DAY DELETE
        </engine>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </blob_storage_log>
</yandex>
EOT

fi

if [[ "$CLICKHOUSE_VERSION" == "head" || "${CLICKHOUSE_VERSION}" =~ ^19\. || "${CLICKHOUSE_VERSION}" =~ ^2[0-9]\.[1-9] ]]; then

cat <<EOT > /etc/clickhouse-server/config.d/merge_tree_low_memory.xml
<yandex>
    <merge_tree>
        <merge_max_block_size>1024</merge_max_block_size>
        <max_bytes_to_merge_at_max_space_in_pool>1073741824</max_bytes_to_merge_at_max_space_in_pool>
        <number_of_free_entries_in_pool_to_lower_max_size_of_merge>0</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
        <number_of_free_entries_in_pool_to_execute_mutation>1</number_of_free_entries_in_pool_to_execute_mutation>
    </merge_tree>
</yandex>
EOT

fi


if [[ "$CLICKHOUSE_VERSION" == "head" || "${CLICKHOUSE_VERSION}" =~ ^2[3]\.[4-9] || "${CLICKHOUSE_VERSION}" =~ ^2[4-9]\.[1-9] ]]; then

cat <<EOT > /etc/clickhouse-server/config.d/merge_tree_low_memory_23.8.xml
<yandex>
    <merge_tree>
        <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>1</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
    </merge_tree>
</yandex>
EOT

fi


if [[ "${CLICKHOUSE_VERSION}" =~ ^20\.[1-3] ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/low_memory_in_usersd.xml
<yandex>
  <profiles>
  <default>
    <background_pool_size>2</background_pool_size>
  </default>
  </profiles>
</yandex>
EOT

cat <<EOT > /etc/clickhouse-server/config.d/low_memory_in_configd.xml
<yandex>
    <tables_loader_foreground_pool_size>0</tables_loader_foreground_pool_size>
    <tables_loader_background_pool_size>0</tables_loader_background_pool_size>
    <background_message_broker_schedule_pool_size>1</background_message_broker_schedule_pool_size>
    <background_common_pool_size>2</background_common_pool_size>
    <background_fetches_pool_size>1</background_fetches_pool_size>
    <background_merges_mutations_scheduling_policy>round_robin</background_merges_mutations_scheduling_policy>
    <background_merges_mutations_concurrency_ratio>2</background_merges_mutations_concurrency_ratio>
</yandex>
EOT

elif [[ "${CLICKHOUSE_VERSION}" =~ ^20\.[4-9] ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/low_memory_in_usersd.xml
<yandex>
  <profiles>
  <default>
    <background_pool_size>2</background_pool_size>
    <background_buffer_flush_schedule_pool_size>1</background_buffer_flush_schedule_pool_size>
  </default>
  </profiles>
</yandex>
EOT

cat <<EOT > /etc/clickhouse-server/config.d/low_memory_in_configd.xml
<yandex>
    <tables_loader_foreground_pool_size>0</tables_loader_foreground_pool_size>
    <tables_loader_background_pool_size>0</tables_loader_background_pool_size>
    <background_message_broker_schedule_pool_size>1</background_message_broker_schedule_pool_size>
    <background_common_pool_size>2</background_common_pool_size>
    <background_fetches_pool_size>1</background_fetches_pool_size>
    <background_merges_mutations_scheduling_policy>round_robin</background_merges_mutations_scheduling_policy>
    <background_merges_mutations_concurrency_ratio>2</background_merges_mutations_concurrency_ratio>
</yandex>
EOT

elif [[ "${CLICKHOUSE_VERSION}" =~ ^21\.[1-9]$ || "${CLICKHOUSE_VERSION}" =~ ^21\.10 ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/low_memory_in_usersd.xml
<yandex>
  <profiles>
  <default>
    <background_pool_size>2</background_pool_size>
    <background_buffer_flush_schedule_pool_size>1</background_buffer_flush_schedule_pool_size>
    <background_message_broker_schedule_pool_size>1</background_message_broker_schedule_pool_size>
    <background_fetches_pool_size>1</background_fetches_pool_size>
  </default>
  </profiles>
</yandex>
EOT

cat <<EOT > /etc/clickhouse-server/config.d/low_memory_in_configd.xml
<yandex>
    <tables_loader_foreground_pool_size>0</tables_loader_foreground_pool_size>
    <tables_loader_background_pool_size>0</tables_loader_background_pool_size>
    <background_merges_mutations_scheduling_policy>round_robin</background_merges_mutations_scheduling_policy>
    <background_merges_mutations_concurrency_ratio>2</background_merges_mutations_concurrency_ratio>
</yandex>
EOT

elif [[ "${CLICKHOUSE_VERSION}" =~ ^21\.1[1-9] ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/low_memory_in_usersd.xml
<yandex>
  <profiles>
  <default>
    <background_pool_size>2</background_pool_size>
    <background_buffer_flush_schedule_pool_size>1</background_buffer_flush_schedule_pool_size>
    <background_message_broker_schedule_pool_size>1</background_message_broker_schedule_pool_size>
    <background_fetches_pool_size>1</background_fetches_pool_size>
    <background_merges_mutations_concurrency_ratio>2</background_merges_mutations_concurrency_ratio>
  </default>
  </profiles>
</yandex>
EOT

cat <<EOT > /etc/clickhouse-server/config.d/low_memory_in_configd.xml
<yandex>
    <tables_loader_foreground_pool_size>0</tables_loader_foreground_pool_size>
    <tables_loader_background_pool_size>0</tables_loader_background_pool_size>
    <background_merges_mutations_scheduling_policy>round_robin</background_merges_mutations_scheduling_policy>
</yandex>
EOT

elif [[ "${CLICKHOUSE_VERSION}" =~ ^22\. ]]; then

cat <<EOT > /etc/clickhouse-server/users.d/low_memory_in_usersd.xml
<yandex>
  <profiles>
  <default>
    <background_pool_size>2</background_pool_size>
    <background_buffer_flush_schedule_pool_size>1</background_buffer_flush_schedule_pool_size>
    <background_message_broker_schedule_pool_size>1</background_message_broker_schedule_pool_size>
    <background_fetches_pool_size>1</background_fetches_pool_size>
    <background_merges_mutations_concurrency_ratio>2</background_merges_mutations_concurrency_ratio>
  </default>
  </profiles>
</yandex>
EOT

cat <<EOT > /etc/clickhouse-server/config.d/low_memory_in_configd.xml
<yandex>
    <tables_loader_foreground_pool_size>0</tables_loader_foreground_pool_size>
    <tables_loader_background_pool_size>0</tables_loader_background_pool_size>
    <background_merges_mutations_scheduling_policy>round_robin</background_merges_mutations_scheduling_policy>
</yandex>
EOT

else

cat <<EOT > /etc/clickhouse-server/config.d/low_memory_in_configd.xml
<yandex>
    <background_pool_size>2</background_pool_size>
    <background_buffer_flush_schedule_pool_size>1</background_buffer_flush_schedule_pool_size>
    <background_merges_mutations_concurrency_ratio>2</background_merges_mutations_concurrency_ratio>
    <background_merges_mutations_scheduling_policy>round_robin</background_merges_mutations_scheduling_policy>
    <background_move_pool_size>1</background_move_pool_size>
    <background_fetches_pool_size>1</background_fetches_pool_size>
    <background_common_pool_size>2</background_common_pool_size>
    <background_schedule_pool_size>8</background_schedule_pool_size>
    <background_message_broker_schedule_pool_size>1</background_message_broker_schedule_pool_size>
    <background_distributed_schedule_pool_size>1</background_distributed_schedule_pool_size>
    <tables_loader_foreground_pool_size>0</tables_loader_foreground_pool_size>
    <tables_loader_background_pool_size>0</tables_loader_background_pool_size>
</yandex>
EOT

fi


if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^2[5-9]\.[0-9]+ ]]; then
cat <<EOT > /etc/clickhouse-server/config.d/user_defined_zookeeper_path.xml
<yandex>
  <user_defined_zookeeper_path>/clickhouse/user_defined</user_defined_zookeeper_path>
</yandex>
EOT
fi


# named_collections_control configuration based on ClickHouse version
if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^23\. || "${CLICKHOUSE_VERSION}" =~ ^24\. || "${CLICKHOUSE_VERSION}" =~ ^2[5-9]\. || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then
cat <<EOT > /etc/clickhouse-server/users.d/named_collection_control.xml
<yandex>
  <users>
  <default>
    <named_collection_control>1</named_collection_control>
    <show_named_collections>1</show_named_collections>
    <show_named_collections_secrets>1</show_named_collections_secrets>
  </default>
  <backup>
    <named_collection_control>1</named_collection_control>
    <show_named_collections>1</show_named_collections>
    <show_named_collections_secrets>1</show_named_collections_secrets>
  </backup>
  </users>
</yandex>
EOT
fi

# named_collections_control configuration based on ClickHouse version
if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^25\.[7-9] || "${CLICKHOUSE_VERSION}" =~ ^25\.1[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^2[6-9]\.[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then
cat <<EOT > /etc/clickhouse-server/config.d/named_collections_storage.xml
<yandex>
  <named_collections_storage>
    <type>keeper_encrypted</type>
    <algorithm>aes_256_ctr</algorithm>
    <key_hex>bebec0cabebec0cabebec0cabebec0cabebec0cabebec0cabebec0cabebec0ca</key_hex>
    <path>/clickhouse/named_collections</path>
  </named_collections_storage>
</yandex>
EOT
elif [[ "${CLICKHOUSE_VERSION}" =~ ^25\.[3-6]+ ]]; then
cat <<EOT > /etc/clickhouse-server/config.d/named_collections_storage.xml
<yandex>
  <named_collections_storage>
    <type>local_encrypted</type>
    <algorithm>aes_128_ctr</algorithm>
    <key_hex>bebec0cabebec0cabebec0cabebec0ca</key_hex>
  </named_collections_storage>
</yandex>
EOT
elif [[ "${CLICKHOUSE_VERSION}" =~ ^24\.[8-9] || "${CLICKHOUSE_VERSION}" =~ ^24\.1[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^25\.[1-2] ]]; then
cat <<EOT > /etc/clickhouse-server/config.d/named_collections_storage.xml
<yandex>
  <named_collections_storage>
    <type>keeper</type>
    <path>/clickhouse/named_collections</path>
  </named_collections_storage>
</yandex>
EOT
elif [[ "${CLICKHOUSE_VERSION}" =~ ^24\.[3-7] ]]; then
cat <<EOT > /etc/clickhouse-server/config.d/named_collections_storage.xml
<yandex>
  <named_collections_storage>
    <type>local</type>
  </named_collections_storage>
</yandex>
EOT
fi

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^21\.[3-9]+ || "${CLICKHOUSE_VERSION}" =~ ^21\.1[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^2[2-9]\.[0-9]+ || "${CLICKHOUSE_VERSION}" =~ ^[3-9] ]]; then
cat <<EOT > /etc/clickhouse-server/users.d/allow_experimental_database_replicated.xml
<yandex>
  <profiles><default><allow_experimental_database_replicated>1</allow_experimental_database_replicated></default></profiles>
</yandex>
EOT
fi

if [[ "${CLICKHOUSE_VERSION}" == "head" || "${CLICKHOUSE_VERSION}" =~ ^2[3-9]\.[0-9]+ ]]; then
cat <<EOT > /etc/clickhouse-server/users.d/database_replicated_allow_replicated_engine_arguments.xml
<yandex>
  <profiles><default><database_replicated_allow_replicated_engine_arguments>1</database_replicated_allow_replicated_engine_arguments></default></profiles>
</yandex>
EOT
fi
