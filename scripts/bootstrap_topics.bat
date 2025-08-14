@echo off
echo Creating Kafka topics...

echo Creating topic: news_headlines
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic news_headlines --replication-factor 1 --partitions 1

echo Creating topic: price_ticks  
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic price_ticks --replication-factor 1 --partitions 1

echo Creating topic: impact_scores
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic impact_scores --replication-factor 1 --partitions 1

echo.
echo Listing all topics:
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --list

echo.
echo Topic creation complete!
pause