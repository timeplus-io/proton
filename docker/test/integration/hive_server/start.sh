service ssh start
sed s/HOSTNAME/$HOSTNAME/ /hadoop-3.1.0/etc/hadoop/core-site.xml.template > /hadoop-3.1.0/etc/hadoop/core-site.xml
hadoop namenode -format
start-all.sh
service mysql start
mysql -u root -e "CREATE USER \"test\"@\"localhost\" IDENTIFIED BY \"test\""
mysql -u root -e "GRANT ALL  ON * . * TO 'test'@'localhost'"
schematool -initSchema -dbType mysql
#nohup hiveserver2 &
nohup hive --service metastore &
bash /prepare_hive_data.sh
while true; do sleep 1000; done
