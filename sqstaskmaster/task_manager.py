import logging
import json

import boto3
from json import JSONDecodeError

logger = logging.getLogger(__name__)


class TaskManager:
    def __init__(self, sqs_url, notify=None):
        self.sqs = boto3.resource("sqs")
        self.url = sqs_url
        self.queue = self.sqs.Queue(self.url)
        self._notify = notify

    def attributes(self):
        """
             'QueueArn': 'ARN_STRING',
             'ApproximateNumberOfMessages': '<int>',
             'ApproximateNumberOfMessagesNotVisible': '<int>',
             'ApproximateNumberOfMessagesDelayed': '<int>',
             'CreatedTimestamp': 'epoch seconds',
             'LastModifiedTimestamp': 'epoch seconds',
             'VisibilityTimeout': '<int>',
             'MaximumMessageSize': '<int>',
             'MessageRetentionPeriod': '<int>',
             'DelaySeconds': '<int>',
             'ReceiveMessageWaitTimeSeconds': '<int>',
             'FifoQueue': 'true/false',
             'ContentBasedDeduplication': 'true/false'
        :return: dict
        """
        self.queue.load()
        return self.queue.attributes

    def purge(self):
        self.queue.purge()

    def submit(self, task, **kwargs):
        body = json.dumps(
            {"task": task, "kwargs": kwargs}, default=lambda o: o.__str__()
        )
        return self.queue.send_message(
            MessageBody=body,
            MessageAttributes={
                "service_name": {
                    "StringValue": "WorkforceOptimizer",
                    "DataType": "String",
                }
            },
        )

    def notify(self, exception, context=None):
        if self._notify:
            self._notify(exception, context=context)

    def task_generator(self, wait_time=20, sqs_timeout=20):
        """
        Run as:


        for task, kwargs, message in TaskManager(sqs_url).task_generator(sqs_timeout=30):

            if task == 'MyTask':
                with MyHandler(message, sqs_timeout=30, alarm_timeout=25, hard_timeout=300, **kwargs) as handler:
                    handler.run()
            elif task == 'MyOtherTask':
                with MyOtherHandler(message, sqs_timeout=30, alarm_timeout=25, hard_timeout=300, **kwargs) as handler:
                    handler.run()
            else:
                # Do something about unexpected task request

        :param wait_time: time to wait for messages if none are immediately available (max is 20 seconds)
        :param sqs_timeout: visibility timeout for processing the message - another worker will retry if this expires
        :return: Iterator[task, kwargs, message]
        """
        if 0 > wait_time or wait_time > 20:
            # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/working-with-messages.html#setting-up-long-polling # noqa: ignore=E501
            raise ValueError(
                "Invalid polling wait time {}; must be between 0 and 20".format(
                    wait_time
                )
            )

        if 0 > sqs_timeout or sqs_timeout > (12 * 60 * 60):
            # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-limits.html
            raise ValueError(
                "Invalid sqs timeout {} seconds; must be between 0 and 12 hours".format(
                    sqs_timeout
                )
            )

        while True:
            messages = self.queue.receive_messages(
                AttributeNames=["All"],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=wait_time,
                VisibilityTimeout=sqs_timeout,
            )  # Do not handle exceptions - bomb out and restart the container process

            if not messages:
                logger.info("Waiting for work from SQS!")

            for message in messages:  # Always length one but use a loop anyway
                logger.debug(
                    "received %s with body %s attrs %s",
                    message,
                    message.body,
                    message.attributes,
                )
                try:
                    content = json.loads(message.body)
                    yield content["task"], content["kwargs"], message
                except JSONDecodeError as e:
                    self.notify(e, context={"body": message.body, **message.attributes})
                    logger.exception(
                        "failed to decode message %s with %s",
                        message.body,
                        message.attributes,
                    )
                except KeyError as e:
                    self.notify(e, context={"body": message.body, **message.attributes})
                    logger.exception(
                        "failed to get required field %s from content %s with %s",
                        e,
                        message.body,
                        message.attributes,
                    )
