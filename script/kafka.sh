# 创建对应的 topic 为 sensor
./kafka-topics.sh --create --bootstrap-server 192.168.235.109:9092,192.168.235.110:9092,192.168.235.111:9092 --replication-factor 3 --partitions 3 --topic sensor

# 对对应的 topic 生产消息
./kafka-console-producer.sh --broker-list 192.168.235.109:9092,192.168.235.110:9092,192.168.235.111:9092 --topic sensor