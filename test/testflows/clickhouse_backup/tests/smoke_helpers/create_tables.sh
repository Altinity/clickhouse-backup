for i in {1..1000}
do
   echo "create $i"
   python3 -c "import helpers;helpers.create_table_query(\"t$i\", 100)" | clickhouse-client -n
done