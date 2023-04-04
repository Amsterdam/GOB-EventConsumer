# GOB-EventConsumer

Consumes events from RabbitMQ and uses these to update the RefDB. Events are published by GOB-EventProducer. Uses the
`EventsProcessor` class from [schema-tools](https://github.com/Amsterdam/schema-tools) to modify the RefDB.

# Environment variables
Example environment variables are set in `.env.example`. Create your own `.env` based on this example file:

```bash
cp .env.example .env
```
To initialise the configuration:

```bash
export $(cat .env | xargs)
```

# Infrastructure

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra)
is required to run this component.

# Docker

## Requirements

* docker compose >= 1.25
* Docker CE >= 18.09

## Run

```bash
docker compose build
docker compose up &
```

## Tests

```bash
docker compose -f src/.jenkins/test/docker-compose.yml build
docker compose -f src/.jenkins/test/docker-compose.yml run --rm test
```

# Local

## Requirements

* Python >= 3.9

## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```

Or activate the previously created virtual environment:

```bash
source venv/bin/activate
```

# Run

Start the service:

```bash
cd src
python -m gobeventproducer
```

## Tests

Run the tests:

```bash
cd src
sh test.sh
```
