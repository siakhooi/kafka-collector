help:
clean:
	rm -rf dist target coverage .coverage \
	src/kafka_collector/__pycache__ .pytest_cache docker-compose/captures \
	tests/__pycache__ .tox *.log output.jsonl download.zip
run:
	poetry run kafka-collector
run-help:
	poetry run kafka-collector -h
# expect error
run-p:
	poetry run kafka-collector -t xx -p
run1:
	poetry run kafka-collector -t topic1,topic2,topic3
run1-o:
	poetry run kafka-collector -t topic1,topic2,topic3 -o output.jsonl
# expect warning
run1-oc:
	poetry run kafka-collector -t topic1,topic2,topic3 -o output.jsonl -c /tmp/kafka-collector
# expect warning
run1-op:
	poetry run kafka-collector -t topic1,topic2,topic3 -o output.jsonl -p 8080
# expect warning
run1-so:
	poetry run kafka-collector -t topic1,topic2,topic3 -o output.jsonl -m service
# expect warning
run1-so1:
	poetry run kafka-collector -t topic1,topic2,topic3 -o - -m service
run1-s:
	poetry run kafka-collector -t topic1,topic2,topic3 -m service
run1-sc:
	poetry run kafka-collector -t topic1,topic2,topic3 -m service -c /tmp/kafka-collector1
run1-es:
	COLLECTOR_MODE=service KAFKA_TOPICS=topic1,topic2,topic3 poetry run kafka-collector

curl-files:
	curl localhost:8080/files |jq
curl-reset:
	curl -X POST localhost:8080/reset
curl-reset-name:
	curl -X POST localhost:8080/reset -H "Content-Type: application/json" -d '{"name":"abc"}'
curl-download:
	curl localhost:8080/download
curl-download-name:
	curl localhost:8080/download?name=abc
curl-download-zip:
	curl localhost:8080/download?type=zip -o download.zip
curl-download-zip-name:
	curl "localhost:8080/download?type=zip&name=abc" -o download.zip
set-version:
	scripts/set-version.sh
build:
	poetry build
install:
	poetry install
flake8:
	poetry run flake8
update:
	poetry update
test:
	 poetry run pytest --capture=sys \
	 --junit-xml=coverage/test-results.xml \
	 --cov=kafka_collector \
	 --cov-report term-missing  \
	 --cov-report xml:coverage/coverage.xml \
	 --cov-report html:coverage/coverage.html \
	 --cov-report lcov:coverage/coverage.info

all: clean set-version install flake8 build tox-run
one: clean set-version install flake8 build
	tox run -e py314
312: clean set-version install flake8 build
	tox run -e py312
docker-build:
	docker build -f docker/Dockerfile -t siakhooi/kafka-collector:latest .
docker-run-cli:
	docker run -p 8080:8080  --network=host  -e KAFKA_TOPICS=topic1,topic2,topic3 siakhooi/kafka-collector:latest
docker-run-service:
	docker run -p 8080:8080  --network=host  -e COLLECTOR_MODE=service -e KAFKA_TOPICS=topic1,topic2,topic3 siakhooi/kafka-collector:latest

release:
	scripts/release.sh

tox-run:
	tox run
