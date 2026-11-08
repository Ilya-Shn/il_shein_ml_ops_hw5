.PHONY: up down retrain check pause resume fraud normal scenarios

up:
	mkdir -p data runtime
	test -s data/train.csv
	docker compose up -d --build --wait

down:
	docker compose down

retrain:
	docker compose exec trainer python -m app.train

check:
	docker compose exec -T scorer python -m tests.check

scenarios:
	python3 tests/scenarios.py

pause:
	docker compose stop producer

resume:
	docker compose start producer

fraud:
	docker compose stop producer
	docker compose run -d --name hw5-fraud-stream --no-deps producer python -m app.producer --fraud-only

normal:
	docker rm -f hw5-fraud-stream
	docker compose start producer
