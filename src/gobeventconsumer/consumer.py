import json
import logging
import re

import pika
from schematools.cli import _get_dataset_schema
from schematools.events.full import EventsProcessor
from schematools.naming import to_snake_case
from schematools.types import DatasetSchema
from sqlalchemy import create_engine

from gobeventconsumer.config import DATABASE_URL, EVENTS_EXCHANGE, SCHEMA_URL

logging.getLogger("pika").setLevel(logging.WARNING)


class GOBEventConsumer:
    """GOBEventConsumer runs a RabbitMQ consumer to receive events with which to update the RefDB."""

    def __init__(self, connection_params: pika.ConnectionParameters, catalogs: list[str]):
        self._connection_params = connection_params
        self._catalogs = catalogs
        self._logger = logging.getLogger("GOBEventConsumer")

    def _connect(self):
        self._logger.info("Connecting to RabbitMQ")
        connection = pika.SelectConnection(parameters=self._connection_params, on_open_callback=self._on_open)

        try:
            connection.ioloop.start()
        except Exception as e:
            self._logger.info(f"Received Exception: {str(e)}. Closing connection and reraising.")
            connection.close()
            raise e

    def _on_open(self, connection):
        connection.channel(on_open_callback=self._on_channel_open)

    def _create_queue_and_consume(self, channel: pika.channel.Channel, queue_name: str, routing_key: str, callback):
        channel.queue_declare(queue_name, durable=True, arguments={"x-single-active-consumer": True})
        channel.queue_bind(exchange=EVENTS_EXCHANGE, queue=queue_name, routing_key=routing_key)
        self._logger.info(f"Bind routing key {routing_key} to queue {queue_name}")

        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        self._logger.info(f"Start listening to queue {queue_name}")

    def _on_channel_open(self, channel: pika.channel.Channel):
        """Create two queues for each catalog.

        For example, for catalog 'nap', we create the following queues:
        - nap: This queue receives messages with routing key nap.* . These are the regular nap objects.
        - nap.rel: This queue receives messages with routing key nap.rel.*, for the relation table events.

        All queues are created with the "single active consumer" flag, so that we can safely run multiple instances of
        this service in parallel.
        """
        channel.basic_qos(prefetch_count=5)

        for catalog_name in self._catalogs:
            dataset_schema = _get_dataset_schema(catalog_name, SCHEMA_URL)
            callback = self._on_message(dataset_schema)

            for table in dataset_schema.get_tables(include_through=True):
                if table.is_through_table:
                    # Is relation table. Create routing key of the form nap.peilmerken.rel.peilmerken_ligtInBouwblok
                    collection_name = to_snake_case(table.parent_table.id)
                    routing_key = f"{catalog_name}.{collection_name}.rel.{table.id}"
                else:
                    # Regular object table. Creating routing key of the form nap.peilmerken
                    collection_name = to_snake_case(table.id)
                    routing_key = f"{catalog_name}.{collection_name}"

                queue_name = f"{EVENTS_EXCHANGE}.{routing_key}"
                self._create_queue_and_consume(
                    channel,
                    queue_name,
                    routing_key,
                    callback,
                )

    def _transform_obj_eventdata(self, dataset_schema, header: dict, data: dict):
        """Transform incoming object table event to the structure as understood by the EventsProcessor.

        Renames the fields in the relations. For example, for a nap_peilmerken object, we have a nested relation
        ligt_in_bouwblok. The incoming keys are tid, id, volgnummer, begin_geldigheid, eind_geldigheid. We want
        id, identificatie, volgnummer, begin/eind_geldigheid. The 'id' in EventsProcessor has the role of the 'tid'.

        The mapping is as follows (EventsProcessor: incoming):

        id: tid,
        identificatie: id ('identificatie' is specific to ligt_in_bouwblok, this depends on the identifier)
        volgnummer: volgnummer
        begin_geldigheid: begin_geldigheid
        eind_geldigheid: eind_geldigheid
        """

        def transform_relation(identifier: str, relation_data: dict):
            mapping = {
                "volgnummer": "volgnummer",
                "begin_geldigheid": "begin_geldigheid",
                "eind_geldigheid": "eind_geldigheid",
                "id": "tid",
                identifier: "id",
            }

            return {k: relation_data.get(v) for k, v in mapping.items()}

        table = dataset_schema.get_table_by_id(header["collection"])
        transformed_relations = {}

        for field in table.fields:
            if field.get("relation"):
                attr_name = to_snake_case(field.get("shortname", field.python_name))
                relation_data = data[attr_name]
                identifier = field.related_table.identifier[0]
                transformed_relations[attr_name] = (
                    transform_relation(identifier, relation_data)
                    if isinstance(relation_data, dict)
                    else [transform_relation(identifier, rel_data) for rel_data in relation_data]
                )

        return {
            **data,
            **transformed_relations,
        }

    def _transform_rel_eventdata(self, dataset_schema, header: dict, data: dict):
        """Transform incoming relation table event to the structure as understood by the EventsProcessor.

        The 'src' and 'dst' keys are renamed to names with the source object and relation name respectively, and a
        composite key is added.

        For example, for nap peilmerken_ligtInBouwblok, the transformed event has the following keys, with the original
        keys from the event on the right hand side.

        peilmerken_identificatie: src_id
        peilmerken_id: src_id (would have been src_id.src_volgnummer if peilmerken would have had states. Note that the
                               incoming event does contain an empty (None) src_volgnummer))
        ligt_in_bouwblok_identificatie: dst_id
        ligt_in_bouwblok_volgnummer: dst_volgnummer
        ligt_in_bouwblok_id: dst_id.dst_volgnummer
        id: id
        begin_geldigheid: begin_geldigheid
        eind_geldigheid: eind_geldigheid

        Note that the EventsProcessor may ignore some keys. For this particular case, peilmerken_identificatie,
        begin_geldigheid and eind_geldigheid are (at this moment) ignored. This may change in the future, and we don't
        want to copy the EventsProcessor logic here, so we just construct all the fields we possibly can.

        """

        def get_transformed_fields(data: dict, src_or_dst: str, new_prefix: str, identifier_fields: list):
            transformed = {
                f"{new_prefix}_id": ".".join(
                    [str(f) for f in [data[f"{src_or_dst}_id"], data[f"{src_or_dst}_volgnummer"]] if f is not None]
                ),
                f"{new_prefix}_{identifier_fields[0]}": data[f"{src_or_dst}_id"],
            }

            if len(identifier_fields) > 1:
                transformed |= {f"{new_prefix}_{identifier_fields[1]}": data[f"{src_or_dst}_volgnummer"]}
            return transformed

        copy_fields = ["id", "begin_geldigheid", "eind_geldigheid"]
        transformed_data = {field: data[field] for field in copy_fields}

        collection, relation_name = header["collection"].split("_")
        relation_name_snake = to_snake_case(relation_name)
        table = dataset_schema.get_table_by_id(collection)

        return {
            **transformed_data,
            **get_transformed_fields(data, "src", collection, table.identifier),
            **get_transformed_fields(
                data, "dst", relation_name_snake, table.get_field_by_id(relation_name).related_table.identifier
            ),
        }

    def _on_message(self, dataset_schema: DatasetSchema):
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            importer = EventsProcessor([dataset_schema], connection)

        def handle_message(channel, method, properties, body):
            rel_pattern = re.compile(r"\w+.\w+.rel.\w+")

            def prepare_event(event: dict):
                header = {
                    **event["header"],
                    "dataset_id": event["header"]["catalog"],
                    "table_id": event["header"]["collection"],
                }
                data = event["data"]
                if rel_pattern.match(method.routing_key):
                    data = self._transform_rel_eventdata(dataset_schema, header, data)
                else:
                    data = self._transform_obj_eventdata(dataset_schema, header, data)

                return header, data

            self._logger.info(f"Received message for catalog {dataset_schema.id} with routing key {method.routing_key}")

            contents = json.loads(body.decode("utf-8"))
            with engine.connect() as connection:
                importer.conn = connection

                if isinstance(contents, list):
                    to_process = [prepare_event(event) for event in contents]
                    importer.process_events(to_process)
                else:
                    header, data = prepare_event(contents)
                    importer.process_event(header, data)

                channel.basic_ack(delivery_tag=method.delivery_tag)
            self._logger.debug("Finished message handling")

        return handle_message

    def consume(self):
        """Start consuming."""
        self._connect()
