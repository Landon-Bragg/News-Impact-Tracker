# PowerShell topic bootstrapper
$topics = @("news_headlines","price_ticks","impact_scores")
foreach ($t in $topics) {
  Write-Host "Creating topic $t (if not exists) ..."
  docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic $t --replication-factor 1 --partitions 1 | Out-Null
}
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
