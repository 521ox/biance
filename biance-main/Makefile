.PHONY: install run lint test docker up down
install:
	pip install -r requirements.txt
	pip install pre-commit && pre-commit install
run:
	uvicorn main:app --host 0.0.0.0 --port 8000
lint:
	pre-commit run -a || true
test:
	pytest -q || true
docker:
	docker build -t mtf-node:latest .
up:
	docker compose up -d --build
down:
	docker compose down
