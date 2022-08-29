read -p "Enter kafka brokder host name: [kafka]" kafka_host_name
kafka_host_name=${kafka_host_name:-kafka}
read -p "Enter kafka broker ip address: " kafka_ip
echo "kafka broker host name = $kafka_host_name, ip = $kafka_ip"
docker exec -u 0 neutron /bin/sh -c "cat /etc/hosts |sed '/kafka/d' >>/etc/hosts_new"
docker exec -u 0 neutron /bin/sh -c "cat /etc/hosts_new > /etc/hosts"
docker exec -u 0 neutron /bin/sh -c "echo '$kafka_ip   $kafka_host_name' >> /etc/hosts"
docker exec -u 0 neutron /bin/sh -c "rm /etc/hosts_new"
docker exec -u 0 filebeat /bin/sh -c "cat /etc/hosts |sed '/kafka/d' >>/etc/hosts_new"
docker exec -u 0 filebeat /bin/sh -c "cat /etc/hosts_new > /etc/hosts"
docker exec -u 0 filebeat /bin/sh -c "echo '$kafka_ip   $kafka_host_name' >> /etc/hosts"
docker exec -u 0 filebeat /bin/sh -c "rm /etc/hosts_new"
