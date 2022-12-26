dirs:
	sudo mkdir -p logs plugins && sudo chmod -R u=rwx,g=rwx,o=rwx logs dags scripts plugins

docker-compose-up:
	docker compose --env-file .env up airflow-init && docker compose --env-file .env up --build -d

up: dirs docker-compose-up

down:
	docker compose --env-file .env down

down-volumes:
	docker-compose --env-file .env down --volumes --remove-orphans