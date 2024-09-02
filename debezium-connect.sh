echo "wait for 60 seconds"
sleep 60


curl -i -X POST -H "Accept: application/json" -H "Content-Type: application/json" \
  connect:8083/connectors/ \
  -d '{
    "name": "sde-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "password",
      "database.password": "password",
      "database.dbname": "data_engineer",
      "database.server.name": "bankserver1",
      "table.whitelist": "bank.holding",
      "topic.prefix": "dbserver1"
    }
  }'
