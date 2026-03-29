help:
clean:
	rm -rf dist target coverage \
	src/kafka_collector/__pycache__ \
	tests/__pycache__
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
curl-files:
	curl localhost:8080/files |jq
curl-reset:
	curl -X POST localhost:8080/reset
curl-reset-name:
	curl -X POST localhost:8080/reset?name=abc
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

release:
	scripts/release.sh

fix-cert:
	pip install pip-system-certs --trusted-host pypi.org --trusted-host files.pythonhosted.org
fix-pyenv:
	 pyenv versions --bare > .python-version
tox-run:
	tox run
