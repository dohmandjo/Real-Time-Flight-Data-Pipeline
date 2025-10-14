up:
\tdocker compose up -d --build
down:
\tdocker compose down -v
logs:
\tdocker compose logs -f --tail=200
psql:
\tdocker exec -it flightpostgres psql -U $$PGUSER -d PGDATABASE
topics:
\tdocker exec -it kafka kafka-topics.sh --bootstarp-server kafka:9094 --list