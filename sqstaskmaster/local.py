import logging
import time
from collections import defaultdict
import queue

logger = logging.getLogger(__name__)
"""
Simple in memory unbounded SimpleQueue based implementation of AWS SQS for local development and integration testing.
Not for use with threads or multiprocessing. MessageHandler class timeouts should behave as expected.
No retry behavior is implemented.
These classes are for synchronous integration testing of message producer and message consumer interface.
These classes are not for use in production or for validating operational behavior of producer and consumer system.

TODO Consider adding thread support by calling task_done in LocalMessage.delete
"""


class LocalMessage:
    """
    Partial implementation of the AWS SQS Message interface for local testing of the message producer and consumer.
    This does not implement message behavior beyond the body and attributes used for encoded content.

    Methods used by the MessageHandler class (change_visibility and delete) are No-ops!
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#message
    """

    def __init__(self, body, attributes, message_attributes):
        """
        This is not the same API as the AWS implementation!
        :param body:
        :param attributes:
        """
        self._body = body
        self._attributes = attributes
        self._message_attributes = message_attributes

    @property
    def attributes(self):
        return self._attributes

    @property
    def body(self):
        return self._body

    @property
    def message_attributes(self):
        return self._message_attributes

    def change_visibility(self, *args, **kwargs):
        logger.info("noop called with %s, %s", args, kwargs)

    def delete(self):
        logger.info("noop delete")

    def __str__(self):
        return "LocalMessage(body: {}; attributes: {}; message_attributes: {})".format(
            self.body, self.attributes, self.message_attributes
        )


class LocalQueue:
    """
    Partial implementation of the SQS Queue interface for local integration testing of the producer and consumer.
    This does not implement behavior beyond the ability to put and get messages in FIFO order to a queue named by a url.

    Some of the SQS features that are missing: Retries, visibility & timeout, thread/process safety.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#queue
    """

    local_queues = defaultdict(queue.SimpleQueue)

    def __init__(self, url):
        self.url = url
        self._queue = self.local_queues[url]

    def load(self):
        logger.info("Noop load!")

    @property
    def queue(self):
        return self._queue

    @property
    def attributes(self):
        logger.info("Noop attributes!")
        # Return partial set of attributes
        return {
            "QueueArn": "LocalQueue replacing: " + self.url,
            "ApproximateNumberOfMessages": self.queue.qsize(),
            "ApproximateNumberOfMessagesNotVisible": 0,
            "ApproximateNumberOfMessagesDelayed": 0,
            "FifoQueue": True,
            "ContentBasedDeduplication": False,
        }

    def purge(self):
        while True:
            try:
                self.queue.get_nowait()
            except queue.Empty:
                break

    def send_message(self, **kwargs):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.send_message
        :param kwargs: expects MessageBody (String) and optional MessageAttributes (Dict)
        :return: partial metadata of actual API
        """
        attributes = {}
        # Ignore attribute behavior which is not relevant
        message = LocalMessage(
            kwargs["MessageBody"], attributes, kwargs.get("MessageAttributes", {})
        )

        logger.debug("Sending message: %s", message)
        self.queue.put_nowait(message)
        return {"MD5OfMessageBody": "Fake LocalMessage MD5 Body"}

    def receive_messages(self, **kwargs):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.receive_messages
        :param kwargs: simulates behavior of WaitTimeSeconds and MaxNumberOfMessages
        :return:
        """
        time_remaining = kwargs.get("WaitTimeSeconds", 20)

        result = []

        tic = time.perf_counter()
        for i in range(kwargs.get("MaxNumberOfMessages", 1)):
            time_remaining = max(0, time_remaining - time.perf_counter() + tic)

            try:
                result.append(self.queue.get(block=True, timeout=time_remaining))
            except queue.Empty:
                break

        return result
