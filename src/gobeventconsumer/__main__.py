import logging

from gobeventconsumer.config import CATALOGS, CONNECTION_PARAMS
from gobeventconsumer.consumer import GOBEventConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def main():
    """Start consume loop."""
    logger.info("Starting consumer")
    consumer = GOBEventConsumer(CONNECTION_PARAMS, CATALOGS)
    consumer.consume()


if __name__ == "__main__":
    main()
