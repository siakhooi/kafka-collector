# kafka-collector

[![License](https://img.shields.io/github/license/siakhooi/kafka-collector)](https://github.com/siakhooi/kafka-collector/blob/main/LICENSE)
[![Release](https://img.shields.io/github/v/release/siakhooi/kafka-collector)](https://github.com/siakhooi/kafka-collector/releases/latest)
![workflow](https://github.com/siakhooi/kafka-collector/actions/workflows/build.yaml/badge.svg)
![workflow](https://github.com/siakhooi/kafka-collector/actions/workflows/workflow-deployments.yml/badge.svg)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-collector&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-collector)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-collector&metric=bugs)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-collector)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-collector&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-collector)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-collector&metric=coverage)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-collector)
[![Wise](https://img.shields.io/badge/Funding-Wise-33cb56.svg?logo=wise)](https://wise.com/pay/me/siakn3)
![visitors](https://hit-tztugwlsja-uc.a.run.app/?outputtype=badge&counter=ghmd-kafka-collector)

Collect Kafka messages from multiple topics. Ships as a **CLI** and a **microservice** (HTTP capture management).

**kafka-collector** is the **first step** in a three-tool workflow to **record**, **reshape**, and **replay** Kafka traffic (often into a **development** cluster). Captures are usually **JSONL**; use the companion repos below to turn them into emulator config and produce messages again.

## Related repositories (recommended workflow)

This project works best **in sequence** with:

| Step  | Repository                                                                 | Role                                                                  |
| ----- | -------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| **1** | **kafka-collector** (this repo)                                            | Collect messages from Kafka topics (CLI + microservice).              |
| **2** | [collector-to-emulator](https://github.com/siakhooi/collector-to-emulator) | Convert collected **JSONL** into files/config usable by the emulator. |
| **3** | [kafka-emulator](https://github.com/siakhooi/kafka-emulator)               | Produce the configured messages onto Kafka (e.g. dev).                |

**Typical flow**

1. Use **kafka-collector** to capture traffic from the topics you care about.
2. Use **collector-to-emulator** to generate an **emulator configuration** from those captures.
3. **Edit the config** as needed (secrets, ordering, scenarios, gaps).
4. Use **kafka-emulator** to **replay** messages to your target Kafka.

Together: **collect → convert → curate → replay**. For flags, modes, and deployment, see the sections below and [Documentation](#documentation).

## Installation
```
pip install kafka_collector
```
## Usage
```
$ kafka-collector -h
usage: kafka-collector [-h] [-v] [-t TOPICS] [-b BOOTSTRAP_SERVER] [-g GROUP]
                       [-o OUTPUT] [-c CAPTURE_DIR] [-m {cli,service}]
                       [-p PORT]
                       [--log-level {debug,info,warning,error,critical}]
                       [--debug]

collect kafka messages from multiple topics

options:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  -t, --topics TOPICS   comma separated list of kafka topics to be listened to
  -b, --bootstrap-server BOOTSTRAP_SERVER
                        kafka bootstrap server (default: localhost:9092)
  -g, --group GROUP     consumer group id (default: random uuid)
  -o, --output OUTPUT   output file path, use '-' for stdout (default: stdout)
  -c, --capture-dir CAPTURE_DIR
                        capture directory for service mode (default:
                        /tmp/kafka-collector)
  -m, --mode {cli,service}
                        run mode: cli or service (default: cli)
  -p, --port PORT       service port for service mode (default: 8080)
  --log-level {debug,info,warning,error,critical}
                        logging level (default: info)
  --debug               enable debug logging (shortcut for --log-level debug)

```
## Environment Variables
- `KAFKA_TOPICS` - comma separated list of topics
- `KAFKA_BOOTSTRAP_SERVER` - bootstrap server address
- `KAFKA_GROUP` - consumer group id
- `COLLECTOR_CAPTURE_DIR` - capture directory for service mode
- `COLLECTOR_MODE` - run mode (cli or service)
- `COLLECTOR_SERVICE_PORT` - service port (must be integer)
- `LOG_LEVEL` - logging level

## Documentation

- [CLI Mode](docs/cli-mode.md) - Default mode for streaming messages to stdout or file
- [Service Mode](docs/service-mode.md) - HTTP service mode with REST API for capture management

## Deployment Examples

- [Docker Compose](docker-compose/) - Run with Docker Compose
- [Kubernetes](kubernetes/) - Deploy to Kubernetes

## Links
- https://pypi.org/project/kafka_collector/
- https://hub.docker.com/r/siakhooi/kafka-collector
- https://github.com/siakhooi/kafka-collector
- https://sonarcloud.io/project/overview?id=siakhooi_kafka-collector
- https://qlty.sh/gh/siakhooi/projects/kafka-collector
