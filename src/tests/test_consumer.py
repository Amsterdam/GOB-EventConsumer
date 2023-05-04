import json
from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobeventconsumer.config import EVENTS_EXCHANGE
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
        gec = GOBEventConsumer(MagicMock(), ["gebieden", "meetbouten"])
        gec._on_message = MagicMock(side_effect=lambda x: f"on_message({x})")
        mock_channel = MagicMock()

        gec._on_channel_open(mock_channel)

        mock_channel.queue_declare.assert_has_calls([
            call("gob.events.gebieden", durable=True, arguments={"x-single-active-consumer": True}),
            call("gob.events.gebieden.rel", durable=True, arguments={"x-single-active-consumer": True}),
            call("gob.events.meetbouten", durable=True, arguments={"x-single-active-consumer": True}),
            call("gob.events.meetbouten.rel", durable=True, arguments={"x-single-active-consumer": True}),
        ])

        self.assertEqual(EVENTS_EXCHANGE, "gob.events")

        mock_channel.queue_bind.assert_has_calls([
            call(exchange=EVENTS_EXCHANGE, queue="gob.events.gebieden", routing_key="gebieden.*"),
            call(exchange=EVENTS_EXCHANGE, queue="gob.events.gebieden.rel", routing_key="gebieden.rel.*"),
            call(exchange=EVENTS_EXCHANGE, queue="gob.events.meetbouten", routing_key="meetbouten.*"),
            call(exchange=EVENTS_EXCHANGE, queue="gob.events.meetbouten.rel", routing_key="meetbouten.rel.*"),
        ])

        mock_channel.basic_consume.assert_has_calls([
            call(queue="gob.events.gebieden", on_message_callback=gec._on_message('gebieden')),
            call(queue="gob.events.gebieden.rel", on_message_callback=gec._on_message('gebieden')),
            call(queue="gob.events.meetbouten", on_message_callback=gec._on_message('meetbouten')),
            call(queue="gob.events.meetbouten.rel", on_message_callback=gec._on_message('meetbouten')),
        ])

    @patch("gobeventconsumer.consumer.create_engine")
    @patch("gobeventconsumer.consumer._get_dataset_schema")
    @patch("gobeventconsumer.consumer.EventsProcessor")
    def test_message_handler(self, mock_event_processor, mock_get_dataset_schema, mock_create_engine, mock_logger):
        gec = GOBEventConsumer(MagicMock(), [])
        mock_connection = mock_create_engine.return_value.connect.return_value.__enter__.return_value

        message_handler = gec._on_message("gebieden")
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

        mock_event_processor.assert_called_with([mock_get_dataset_schema.return_value], mock_connection)
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
    @patch("gobeventconsumer.consumer._get_dataset_schema")
    @patch("gobeventconsumer.consumer.EventsProcessor")
    def test_message_handler_batch(self, mock_event_processor, mock_get_dataset_schema, mock_create_engine, mock_logger):
        gec = GOBEventConsumer(MagicMock(), [])
        mock_connection = mock_create_engine.return_value.connect.return_value.__enter__.return_value

        message_handler = gec._on_message("gebieden")
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

        mock_event_processor.assert_called_with([mock_get_dataset_schema.return_value], mock_connection)
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

        message_handler = gec._on_message("nap")
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

        message_handler = gec._on_message("nap")
        mock_create_engine.assert_called_once()

        method = MagicMock()
        method.routing_key = "nap.rel.peilmerken_ligtInBouwblok"
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
