DOCKER_COMPOSE_FILE=docker-compose.yml
ENV_FILE=.env.local

build:
	docker compose -f ${DOCKER_COMPOSE_FILE} --env-file ${ENV_FILE} build

up:
	docker compose -f ${DOCKER_COMPOSE_FILE} --env-file ${ENV_FILE} up -d

down:
	docker compose -f ${DOCKER_COMPOSE_FILE} --env-file ${ENV_FILE} down

prune:
	docker system prune -f

.PHONY: up down prune