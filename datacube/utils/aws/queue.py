# This file is part of the Open Data Cube, see https://opendatacube.org for more information
#
# Copyright (c) 2015-2025 ODC Contributors
# SPDX-License-Identifier: Apache-2.0
import itertools
import json
import logging
from collections.abc import Generator, Iterable, Iterator, Mapping
from typing import Any

import boto3
from toolz import dicttoolz

_LOG: logging.Logger = logging.getLogger(__name__)


class ODCSQSError(Exception):
    """Something wrong with ODC/AWS SQS handling"""


def redrive_queue(
    queue_name: str,
    to_queue_name: str | None = None,
    limit: int | None = None,
    dryrun: bool = False,
    max_wait: int = 5,
    messages_per_request: int = 10,
) -> int:
    """
    Redrive messages from one queue to another. Default usage is to define
    a "deadletter" queue, and pick its "alive" counterpart, and redrive
    messages to that queue.
    """

    def post_messages(to_queue, messages) -> list:
        message_bodies = [
            {"Id": str(n), "MessageBody": m.body} for n, m in enumerate(messages)
        ]
        to_queue.send_messages(Entries=message_bodies)
        # Delete after sending, not before
        for message in messages:
            message.delete()
        return []

    dead_queue = get_queue(queue_name)

    if to_queue_name is not None:
        alive_queue = get_queue(to_queue_name)
    else:
        source_queues = list(dead_queue.dead_letter_source_queues.all())
        if len(source_queues) == 0:
            raise ODCSQSError(
                "No alive queue found for the deadletter queue, please check your configuration."
            )
        if len(source_queues) > 1:
            raise ODCSQSError(
                "Deadletter queue has more than one source, please specify the target queue name."
            )
        alive_queue = source_queues[0]

    messages = get_messages(
        dead_queue,
        limit=limit,
        max_wait=max_wait,
        messages_per_request=messages_per_request,
    )
    count_messages = 0
    approx_n_messages = dead_queue.attributes.get("ApproximateNumberOfMessages")
    try:
        count_messages = int(approx_n_messages)
    except TypeError:
        _LOG.warning("Couldn't get approximate number of messages, setting to 0")

    # If there are no messages then there's no work to do. If it's a dryrun, we
    # don't do anything either.
    if count_messages == 0 or dryrun:
        return count_messages

    count = 0
    message_group = []
    for message in messages:
        # Assume this works. Exception handling elsewhere.
        message_group.append(message)
        count += 1

        if count % 10 == 0:
            message_group = post_messages(alive_queue, message_group)

    # Post the last few messages
    if len(message_group) > 0:
        _ = post_messages(alive_queue, message_group)

    # Return the number of messages that were re-driven.
    return count


def get_queue(queue_name: str):
    """
    Return a queue resource by name, e.g., alex-really-secret-queue
    """
    return boto3.resource("sqs").get_queue_by_name(QueueName=queue_name)


def get_queues(prefix: str | None = None, contains: str | None = None) -> Generator:
    """
    Return a list of sqs queues which the user is allowed to see and filtered by
    the parameters provided
    """
    queues = boto3.resource("sqs").queues.all()
    if prefix is not None:
        queues = queues.filter(QueueNamePrefix=prefix)

    if contains is not None:
        for queue in queues:
            if contains in queue.attributes.get("QueueArn").split(":")[-1]:
                yield queue
    else:
        yield from queues


def publish_message(
    queue, message: str, message_attributes: Mapping[str, Any] | None = None
) -> None:
    """
    Publish a message to a queue resource. Message should be a JSON object dumped as a
    string.
    """
    if message_attributes is None:
        message_attributes = {}
    queue.send_message(
        QueueUrl=queue.url, MessageBody=message, MessageAttributes=message_attributes
    )


def publish_messages(queue, messages) -> None:
    """
    Publish messages to a queue resource.
    """
    queue.send_messages(Entries=messages)


def _sqs_message_stream(queue, **kw) -> Generator:
    while True:
        messages = queue.receive_messages(**kw)
        if len(messages) == 0:
            return

        yield from messages


def get_messages(
    queue,
    limit: int | None = None,
    visibility_timeout: int = 60,
    message_attributes: Iterable[str] | None = None,
    max_wait: int = 1,
    messages_per_request: int = 1,
    **kw,
) -> Iterator:
    """
    Get messages from SQS queue resource. Returns a lazy sequence of message objects.

    :queue: queue URL
    :param limit: the maximum number of messages to return from the queue (default to all)
    :param visibility_timeout: A period of time in seconds during which Amazon SQS prevents other consumers
                               from receiving and processing the message
    :param message_attributes: Select what attributes to include in the messages, default All
    :param max_wait: Longest to wait in seconds before assuming queue is empty (default: 10)
    :param messages_per_request:
    :**kw: Any other arguments are passed to ``.receive_messages()`` boto3 call

    :return: Iterator of sqs messages
    """
    if message_attributes is None:
        message_attributes = ["All"]

    messages = _sqs_message_stream(
        queue,
        VisibilityTimeout=visibility_timeout,
        MaxNumberOfMessages=messages_per_request,
        WaitTimeSeconds=max_wait,
        MessageAttributeNames=message_attributes,
        **kw,
    )

    if limit is None:
        return messages

    if limit < 1:
        raise ODCSQSError(f"Limit {limit} is not valid.")

    return itertools.islice(messages, limit)


def capture_attributes(action: str, stac: dict) -> dict:
    """Determine SNS message attributes"""
    product = dicttoolz.get_in(["properties", "odc:product"], stac)
    date_time = dicttoolz.get_in(["properties", "datetime"], stac)
    maturity = dicttoolz.get_in(["properties", "dea:dataset_maturity"], stac)

    if not product:
        product = stac.get("collection")

    return {
        "action": {"DataType": "String", "StringValue": action},
        "product": {"DataType": "String", "StringValue": product},
        "datetime": {"DataType": "String", "StringValue": date_time},
        **(
            {"maturity": {"DataType": "String", "StringValue": maturity}}
            if maturity
            else {}
        ),
    }


def publish_to_topic(arn: str, action: str, stac: dict) -> None:
    """
    Publish 'added' or 'archived' action to the provided sns topic
    """
    boto3.client("sns").publish(
        TopicArn=arn,
        Message=json.dumps(stac),
        MessageAttributes=capture_attributes(action, stac),
    )
