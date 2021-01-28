# 创建对应的 topic 为 sensor
./kafka-topics.sh --create --bootstrap-server 192.168.235.109:9092,192.168.235.110:9092,192.168.235.111:9092 --replication-factor 3 --partitions 3 --topic sensor

# 查看已有的 topic
./kafka-console-producer.sh --broker-list 192.168.235.109:9092,192.168.235.110:9092,192.168.235.111:9092 --topic sensor