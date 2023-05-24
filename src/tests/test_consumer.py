import json
from unittest import TestCase
from unittest.mock import patch, MagicMock, call
from schematools.cli import _get_dataset_schema

from gobeventconsumer.config import EVENTS_EXCHANGE, SCHEMA_URL
from gobeventconsumer.consumer import GOBEventConsumer


@patch("gobeventconsumer.consumer.logging.getLogger")
class TestGOBEventConsumer(TestCase):

    @patch("gobeventconsumer.consumer.pika.SelectConnection")
    def test_connect(self, mock_connection, mock_logger):
        gec = GOBEventConsumer(MagicMock(), ['gebieden', 'meetbouten'])
        gec._connect()

        mock_connection.assert_called_with(parameters=gec._connection_params, on_open_callback=gec._on_open)
        mock_connection.return_value.ioloop.start.assert_called_once()

    @patch("gobeventconsumer.consumer.pika.SelectConnection")
    def test_connect_ioloop_exception(self, mock_connection, mock_logger):
        gec = GOBEventConsumer(MagicMock(), ['gebieden', 'meetbouten'])

        mock_connection.return_value.ioloop.start.side_effect = Exception("Oh no, this is disturbing!")

        with self.assertRaisesRegex(Exception, "Oh no, this is disturbing!"):
            gec._connect()

        mock_connection.return_value.close.assert_called_once()
        mock_logger.return_value.info.assert_called_with("Received Exception: Oh no, this is disturbing!. Closing connection and reraising.")

    def test_on_open(self, mock_logger):
        gec = GOBEventConsumer(MagicMock(), [])
        mock_connection = MagicMock()
        gec._on_open(mock_connection)
        mock_connection.channel.assert_called_with(on_open_callback=gec._on_channel_open)

    def test_on_channel_open(self, mock_logger):
        gec = GOBEventConsumer(MagicMock(), ["meetbouten"])
        gec._on_message = MagicMock()
        mock_channel = MagicMock()

        gec._on_channel_open(mock_channel)

        mock_channel.basic_qos.assert_called_once_with(prefetch_count=5)

        routing_keys = [
            "meetbouten.meetbouten",
            "meetbouten.metingen",
            "meetbouten.referentiepunten",
            "meetbouten.rollagen",
            "meetbouten.meetbouten.rel.meetbouten_ligtInBouwblok",
            "meetbouten.meetbouten.rel.meetbouten_ligtInBuurt",
            "meetbouten.meetbouten.rel.meetbouten_ligtInStadsdeel",
            "meetbouten.metingen.rel.metingen_refereertAanReferentiepunten",
            "meetbouten.referentiepunten.rel.referentiepunten_ligtInBouwblok",
            "meetbouten.referentiepunten.rel.referentiepunten_ligtInBuurt",
            "meetbouten.referentiepunten.rel.referentiepunten_ligtInStadsdeel",
            "meetbouten.rollagen.rel.rollagen_isGemetenVanBouwblok",
        ]

        queue_declare_calls = []
        queue_bind_calls = []
        basic_consume_calls = []

        for routing_key in routing_keys:
            queue_declare_calls.append(call(f"gob.events.{routing_key}", durable=True, arguments={"x-single-active-consumer": True}))
            queue_bind_calls.append(call(exchange=EVENTS_EXCHANGE, queue=f"gob.events.{routing_key}", routing_key=routing_key))
            basic_consume_calls.append(call(queue=f"gob.events.{routing_key}", on_message_callback=gec._on_message.return_value))

        self.assertEqual(EVENTS_EXCHANGE, "gob.events")

        mock_channel.queue_declare.assert_has_calls(queue_declare_calls)
        mock_channel.queue_bind.assert_has_calls(queue_bind_calls)
        mock_channel.basic_consume.assert_has_calls(basic_consume_calls)

    @patch("gobeventconsumer.consumer.create_engine")
    @patch("gobeventconsumer.consumer.EventsProcessor")
    def test_message_handler(self, mock_event_processor, mock_create_engine, mock_logger):
        gec = GOBEventConsumer(MagicMock(), [])
        mock_connection = mock_create_engine.return_value.connect.return_value.__enter__.return_value

        mock_dataset_schema = MagicMock()
        mock_dataset_schema.id = "gebieden"
        message_handler = gec._on_message(mock_dataset_schema)
        mock_create_engine.assert_called_once()

        method = MagicMock()
        method.routing_key = "gebieden.bouwblokken"
        body = bytes(json.dumps({
            "header": {
                "catalog": "gebieden",
                "collection": "bouwblokken",
                "event_id": 1844,
            },
            "data": {
                "some": "data"
            }
        }), "utf-8")
        channel = MagicMock()

        message_handler(channel, method, {}, body)

        mock_event_processor.assert_called_with([mock_dataset_schema], mock_connection)
        mock_event_processor.return_value.process_event.assert_called_with(
            {
                "catalog": "gebieden",
                "collection": "bouwblokken",
                "event_id": 1844,
                "dataset_id": "gebieden",
                "table_id": "bouwblokken",
            },
            {
                "some": "data",
            }
        )

    @patch("gobeventconsumer.consumer.create_engine")
    @patch("gobeventconsumer.consumer.EventsProcessor")
    def test_message_handler_batch(self, mock_event_processor, mock_create_engine, mock_logger):
        gec = GOBEventConsumer(MagicMock(), [])
        mock_connection = mock_create_engine.return_value.connect.return_value.__enter__.return_value

        mock_dataset_schema = MagicMock()
        mock_dataset_schema.id = "gebieden"
        message_handler = gec._on_message(mock_dataset_schema)
        mock_create_engine.assert_called_once()

        method = MagicMock()
        method.routing_key = "gebieden.bouwblokken"
        body = bytes(json.dumps([{
            "header": {
                "catalog": "gebieden",
                "collection": "bouwblokken",
                "event_id": 1844,
            },
            "data": {
                "some": "data"
            }
        }, {
            "header": {
                "catalog": "gebieden",
                "collection": "bouwblokken",
                "event_id": 1845,
            },
            "data": {
                "some": "other data"
            }
        },
        ]), "utf-8")
        channel = MagicMock()

        message_handler(channel, method, {}, body)

        mock_event_processor.assert_called_with([mock_dataset_schema], mock_connection)
        mock_event_processor.return_value.process_events.assert_called_with([
            ({
                "catalog": "gebieden",
                "collection": "bouwblokken",
                "event_id": 1844,
                "dataset_id": "gebieden",
                "table_id": "bouwblokken",
            },
            {
                "some": "data",
            }),
            ({
                "catalog": "gebieden",
                "collection": "bouwblokken",
                "event_id": 1845,
                "dataset_id": "gebieden",
                "table_id": "bouwblokken",
            },
            {
                "some": "other data",
            })
        ])

    @patch("gobeventconsumer.consumer.create_engine")
    @patch("gobeventconsumer.consumer.EventsProcessor")
    def test_message_handler_obj_event_reldata(self, mock_event_processor, mock_create_engine, mock_logger):
        gec = GOBEventConsumer(MagicMock(), [])

        dataset_schema = _get_dataset_schema("nap", SCHEMA_URL)
        message_handler = gec._on_message(dataset_schema)
        mock_create_engine.assert_called_once()

        method = MagicMock()
        method.routing_key = "nap.peilmerken"
        body = bytes(json.dumps({
            "header": {
                "catalog": "nap",
                "collection": "peilmerken",
                "event_id": 1844,
            },
            "data": {
                "ligt_in_bouwblok": {
                    "tid": "100.1",
                    "id": "100",
                    "begin_geldigheid": "2022-02-02 00:01:02",
                    "eind_geldigheid": None,
                    "volgnummer": 3,
                },
                "some": "other",
                "data": {
                    "etc": "more",
                }
            }
        }), "utf-8")
        channel = MagicMock()

        message_handler(channel, method, {}, body)

        mock_event_processor.return_value.process_event.assert_called_with(
            {
                "catalog": "nap",
                "collection": "peilmerken",
                "event_id": 1844,
                "dataset_id": "nap",
                "table_id": "peilmerken",
            },
            {
                "ligt_in_bouwblok": {
                    "id": "100.1",
                    "identificatie": "100",
                    "begin_geldigheid": "2022-02-02 00:01:02",
                    "eind_geldigheid": None,
                    "volgnummer": 3,
                },
                "some": "other",
                "data": {
                    "etc": "more",
                },
            }
        )


    @patch("gobeventconsumer.consumer.create_engine")
    @patch("gobeventconsumer.consumer.EventsProcessor")
    def test_message_handler_rel_event(self, mock_event_processor, mock_create_engine, mock_logger):
        gec = GOBEventConsumer(MagicMock(), [])

        dataset_schema = _get_dataset_schema("nap", SCHEMA_URL)
        message_handler = gec._on_message(dataset_schema)
        mock_create_engine.assert_called_once()

        method = MagicMock()
        method.routing_key = "nap.peilmerken.rel.peilmerken_ligtInBouwblok"
        body = bytes(json.dumps({
            "header": {
                "catalog": "nap",
                "collection": "peilmerken_ligtInBouwblok",
                "event_id": 1844,
            },
            "data": {
                "id": 24802,
                "src_id": "2148",
                "src_volgnummer": None,
                "dst_id": "298",
                "dst_volgnummer": 2,
                "begin_geldigheid": "2022-02-02 00:01:02",
                "eind_geldigheid": None,
            }
        }), "utf-8")
        channel = MagicMock()

        message_handler(channel, method, {}, body)

        mock_event_processor.return_value.process_event.assert_called_with(
            {
                "catalog": "nap",
                "collection": "peilmerken_ligtInBouwblok",
                "event_id": 1844,
                "dataset_id": "nap",
                "table_id": "peilmerken_ligtInBouwblok",
            },
            {
                "id": 24802,
                "peilmerken_id": "2148",
                "peilmerken_identificatie": "2148",
                "ligt_in_bouwblok_id": "298.2",
                "ligt_in_bouwblok_identificatie": "298",
                "ligt_in_bouwblok_volgnummer": 2,
                "begin_geldigheid": "2022-02-02 00:01:02",
                "eind_geldigheid": None,
            }
        )

    def test_consume(self, mock_logger):
        gec = GOBEventConsumer(MagicMock(), [])
        gec._connect = MagicMock()
        gec.consume()
        gec._connect.assert_called_once()
