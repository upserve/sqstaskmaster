import logging
import sys
import time
import unittest
from _queue import SimpleQueue
from unittest.mock import patch, call

from callee import InstanceOf

from sqstaskmaster.local import LocalMessage, LocalQueue
from sqstaskmaster.message_handler import MessageHandler
from sqstaskmaster.task_manager import TaskManager

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.INFO)


class TestLocalMessage(unittest.TestCase):
    def setUp(self):
        self.instance = LocalMessage("body", {"some": "attr"}, {"some": "msg_attr"})

    def test_init_properties(self):
        self.assertEqual(self.instance.body, "body")
        self.assertDictEqual(self.instance.attributes, {"some": "attr"})
        self.assertDictEqual(self.instance.message_attributes, {"some": "msg_attr"})

    @patch("sqstaskmaster.local.logger.info")
    def test_change_visibility(self, mock_logger):
        self.instance.change_visibility("arg1", somekwarg=5)
        mock_logger.assert_called_once_with(
            "noop called with %s, %s", ("arg1",), {"somekwarg": 5}
        )

    @patch("sqstaskmaster.local.logger.info")
    def test_delete(self, mock_logger):
        self.instance.delete()
        mock_logger.assert_called_once_with("noop delete")


class TestLocalQueue(unittest.TestCase):
    def setUp(self):
        LocalQueue.local_queues.clear()

    def tearDown(self):
        LocalQueue.local_queues.clear()

    def test___init__(self):

        lq1 = LocalQueue("url1")

        self.assertEqual(lq1.url, "url1")
        self.assertIsInstance(lq1.queue, SimpleQueue)

        self.assertDictEqual(LocalQueue.local_queues, {"url1": lq1.queue})

        lq2 = LocalQueue("url2")

        self.assertEqual(lq2.url, "url2")
        self.assertIsInstance(lq2.queue, SimpleQueue)

        self.assertDictEqual(
            LocalQueue.local_queues, {"url1": lq1.queue, "url2": lq2.queue}
        )

        self.assertIsNot(lq1.queue, lq2.queue)

    @patch("sqstaskmaster.local.logger.info")
    def test_load(self, mock_logger):
        LocalQueue("url1").load()
        mock_logger.assert_called_once_with("Noop load!")

    def test_attributes(self):
        self.assertDictEqual(
            LocalQueue("url1").attributes,
            {
                "ApproximateNumberOfMessages": 0,
                "ApproximateNumberOfMessagesDelayed": 0,
                "ApproximateNumberOfMessagesNotVisible": 0,
                "ContentBasedDeduplication": False,
                "FifoQueue": True,
                "QueueArn": "LocalQueue replacing: url1",
            },
        )

    def test_purge(self):
        lq = LocalQueue("url1")
        lq.purge()
        lq.send_message(MessageBody="the message body")
        lq.send_message(MessageBody="the message body")
        self.assertFalse(lq.queue.empty(), "Added a message - should not be empty")
        lq.purge()
        self.assertTrue(lq.queue.empty(), "Purged messages - should be empty")
        # Does not raise or block when empty
        lq.purge()

    @patch("sqstaskmaster.local.LocalMessage")
    def test_send_message(self, mock_cls):
        with patch.object(LocalQueue, "queue") as mock_queue:
            lq = LocalQueue("url1")
            lq.send_message(MessageBody="the message body")
            lq.send_message(
                MessageBody="the second message body", MessageAttributes={"some": "kwd"}
            )

            # Will fail with additional __str__ calls if run at DEBUG
            mock_cls.assert_has_calls(
                [
                    call("the message body", {}, {}),
                    call("the second message body", {}, {"some": "kwd"}),
                ]
            )

            mock_queue.put_nowait.assert_has_calls(
                [call(mock_cls.return_value), call(mock_cls.return_value)]
            )

    @patch("sqstaskmaster.local.LocalMessage")
    def test_receive_messages(self, mock_cls):

        lq = LocalQueue("url1")

        for _ in range(5):
            lq.send_message(MessageBody="the message body")

        result = lq.receive_messages(WaitTimeSeconds=0, MaxNumberOfMessages=3)
        self.assertListEqual(result, [mock_cls.return_value for _ in range(3)])

        result = lq.receive_messages(WaitTimeSeconds=0, MaxNumberOfMessages=3)
        self.assertListEqual(result, [mock_cls.return_value for _ in range(2)])

    @patch("sqstaskmaster.local.LocalMessage")
    def test_receive_messages_timeout(self, mock_cls):
        lq = LocalQueue("url1")
        lq.send_message(MessageBody="the message body")

        tic = time.perf_counter()
        result = lq.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=3)
        toc = time.perf_counter()

        self.assertListEqual(result, [mock_cls.return_value])
        self.assertGreater(toc - tic, 1.0, "Should wait upto 1 second")
        self.assertLess(
            toc - tic, 1.05, "Should not take more than 5/100th of second to return"
        )


TASK_NAME, TASK_STOP = "special_task", "STOP"


def example_producer(url, messages):
    """
    Typically the producer is a runnable main that is scheduled, triggered or run manually to enqueue tasks
    :param url: the sqs url
    :param messages: the tuple of TASK and KWARGS
    """
    tm = TaskManager(url, queue_constructor=LocalQueue)
    for task, kwargs in messages:
        tm.submit(task, **kwargs)
    logger.info("Submitted messages")


class MyHandler(MessageHandler):
    def __init__(
        self,
        message,
        sqs_timeout,
        alarm_timeout,
        run_method,
        hard_timeout=4 * 60 * 60,
        **kwargs
    ):
        super().__init__(message, sqs_timeout, alarm_timeout, hard_timeout)
        self.kwargs = kwargs
        self.run_method = run_method

    def running(self):
        return True

    def run(self):
        self.run_method(**self.kwargs)

    def notify(self, exception, context=None):
        pass


def example_consumer(url, do_method):
    """
    Consumer is typically a runnable main that is deployed to execute a tasks.
    This method demonstrates how the integration with the message publisher might be tested
    :param url: the sqs url
    :param do_method: hook to provide a method that allows message content validation.
    """
    tm = TaskManager(url, queue_constructor=LocalQueue)

    for task, kwargs, message in tm.task_generator(sqs_timeout=30, wait_time=1):
        logger.info("got task %s with %s and %s", task, kwargs, message)
        if task == TASK_NAME:
            with MyHandler(
                message,
                sqs_timeout=5,
                alarm_timeout=1,
                run_method=do_method,
                hard_timeout=1,
                **kwargs
            ) as handler:
                logger.info("Calling run method")
                handler.run()
        elif task == TASK_STOP:
            # typically a deployed consumer would run indefinitely waiting for work
            logger.info("Calling Break for STOP task!")
            break
        else:
            logger.info("Got unexpected task name: %s with message %s", task, message)


class TestLocalIntegration(unittest.TestCase):
    def test_producer_consumer(self):
        messages = [
            (TASK_NAME, {"some": "value"}),
            (TASK_NAME, {"some": "other value"}),
            (TASK_NAME, {"some": "more values"}),
            (TASK_NAME, {"some": None}),
            (TASK_STOP, {"more": "nonesense"}),
        ]

        example_producer("a_particular_queue", messages)
        received = []

        def receiver(**kwargs):
            logger.info("receiver method got %s", kwargs)
            received.append(kwargs)

        example_consumer("a_particular_queue", receiver)

        self.assertListEqual([k for t, k in messages if t == TASK_NAME], received)

    @patch("sqstaskmaster.message_handler.logger.exception")
    def test_producer_consumer_timeout(self, mock_logger):
        messages = [(TASK_NAME, {"some": None}), (TASK_STOP, {"more": "nonesense"})]

        example_producer("a_particular_queue", messages)
        received = []

        def receiver(**kwargs):
            logger.info("receiver method got %s", kwargs)
            received.append(kwargs)
            time.sleep(2)

        example_consumer("a_particular_queue", receiver)

        # Since received is appended before the sleep, the kwarg key is present
        self.assertListEqual([k for t, k in messages if t == TASK_NAME], received)
        # The task will timeout and the handler will log & notify, then proceed to the next message (STOP in this case)
        mock_logger.assert_called_once_with(
            "Failed for message: %s", InstanceOf(LocalMessage)
        )
