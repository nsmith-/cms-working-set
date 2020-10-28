hdfs dfs -rm -R hdfs://analytix/user/ncsmith/working_set_cmssw
spark-submit --packages com.databricks:spark-avro_2.11:4.0.0 generate_working_set.py --source cmssw --out hdfs://analytix/user/ncsmith/working_set_cmssw
hdfs dfs -cp hdfs://analytix/user/ncsmith/working_set_cmssw root://eosuser/eos/user/n/ncsmith/hadoop-transfer/

hdfs dfs -rm -R hdfs://analytix/user/ncsmith/working_set_fwjr
spark-submit --packages com.databricks:spark-avro_2.11:4.0.0 generate_working_set.py --source fwjr --out hdfs://analytix/user/ncsmith/working_set_fwjr
hdfs dfs -cp hdfs://analytix/user/ncsmith/working_set_fwjr root://eosuser/eos/user/n/ncsmith/hadoop-transfer/

hdfs dfs -rm -R hdfs://analytix/user/ncsmith/working_set_classads
spark-submit --packages com.databricks:spark-avro_2.11:4.0.0 generate_working_set.py --source classads --out hdfs://analytix/user/ncsmith/working_set_classads
hdfs dfs -cp hdfs://analytix/user/ncsmith/working_set_classads root://eosuser/eos/user/n/ncsmith/hadoop-transfer/

hdfs dfs -cp hdfs://analytix/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/BLOCKS/part-m-00000 root://eosuser/eos/user/n/ncsmith/hadoop-transfer/dbs_blocks.csv
