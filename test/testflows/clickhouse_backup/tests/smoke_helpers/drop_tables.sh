for i in {1..1000}
do
   echo "drop $i\00"
   python3 -c "import helpers;helpers.drop_table_query(\"t$i\", 100)" | clickhouse-client -n
done