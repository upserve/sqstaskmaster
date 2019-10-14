import unittest
from datetime import date
from json import JSONDecodeError

from unittest.mock import patch, Mock
from callee import InstanceOf

from sqstaskmaster.task_manager import TaskManager


@patch("boto3.resource")
class TestBatchJobQueue(unittest.TestCase):
    SQS_URL = "https://sqs.us-east-1.amazonaws.com/{account_id}/queue_name"

    def test_attributes(self, mock_resource):
        instance = TaskManager(self.SQS_URL)
        self.assertEqual(
            mock_resource.return_value.Queue.return_value.attributes,
            instance.attributes(),
        )
        mock_resource.return_value.Queue.return_value.load.assert_called_once_with()

    def test_purge(self, mock_resource):
        self.assertIsNone(TaskManager(self.SQS_URL).purge())
        mock_resource.return_value.Queue.return_value.purge.assert_called_once_with()

    def test_submit(self, mock_resource):
        instance = TaskManager(self.SQS_URL)
        self.assertEqual(
            mock_resource.return_value.Queue.return_value.send_message.return_value,
            instance.submit("task_name", foo="bar", date=date(2019, 1, 1)),
        )
        mock_resource.return_value.Queue.return_value.send_message.assert_called_once_with(
            MessageBody='{"task": "task_name", "kwargs": {"foo": "bar", "date": "2019-01-01"}}',
            MessageAttributes={
                "service_name": {
                    "StringValue": "WorkforceOptimizer",
                    "DataType": "String",
                }
            },
        )

    def test_task_generator(self, mock_resource):
        instance = TaskManager(self.SQS_URL)

        mock_message = Mock()
        mock_message.body = '{"task": "task_name", "kwargs": {"foo": "bar"}}'
        mock_resource.return_value.Queue.return_value.receive_messages.return_value = [
            mock_message
        ]
        gen = instance.task_generator(wait_time=15, sqs_timeout=10)

        self.assertEqual(("task_name", {"foo": "bar"}, mock_message), next(gen))
        self.assertEqual(("task_name", {"foo": "bar"}, mock_message), next(gen))
        self.assertEqual(("task_name", {"foo": "bar"}, mock_message), next(gen))

        mock_resource.return_value.Queue.return_value.receive_messages.assert_called_with(
            AttributeNames=["All"],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=15,
            VisibilityTimeout=10,
        )

    @patch("sqstaskmaster.task_manager.logger")
    def test_task_generator_decode_error(self, mock_log, mock_resource):
        mock_notify = Mock()
        instance = TaskManager(self.SQS_URL, notify=mock_notify)

        first_mock_message = Mock()
        first_mock_message.body = '{"task": "task_name", "kwargs": {"foo": "b'
        first_mock_message.attributes = {}

        second_mock_message = Mock()
        second_mock_message.body = '{"task": "task_name", "kwargs": {"foo": "bar"}}'

        results = [[first_mock_message], [second_mock_message]]

        mock_resource.return_value.Queue.return_value.receive_messages.side_effect = (
            results
        )
        gen = instance.task_generator(wait_time=15, sqs_timeout=10)

        self.assertEqual(("task_name", {"foo": "bar"}, second_mock_message), next(gen))

        mock_notify.assert_called_once_with(
            InstanceOf(JSONDecodeError),
            context={"body": '{"task": "task_name", "kwargs": {"foo": "b'},
        )
        mock_log.exception.assert_called_once_with(
            "failed to decode message %s with %s",
            '{"task": "task_name", "kwargs": {"foo": "b',
            {},
        )

        mock_resource.return_value.Queue.return_value.receive_messages.assert_called_with(
            AttributeNames=["All"],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=15,
            VisibilityTimeout=10,
        )

    @patch("sqstaskmaster.task_manager.logger")
    def test_task_generator_key_error(self, mock_log, mock_resource):
        mock_notify = Mock()

        instance = TaskManager(self.SQS_URL, notify=mock_notify)

        first_mock_message = Mock()
        first_mock_message.body = '{"wrong_key": "task_name", "kwargs": {"foo": "bar"}}'
        first_mock_message.attributes = {}

        second_mock_message = Mock()
        second_mock_message.body = '{"task": "task_name", "kwargs": {"foo": "bar"}}'

        results = [[first_mock_message], [second_mock_message]]

        mock_resource.return_value.Queue.return_value.receive_messages.side_effect = (
            results
        )
        gen = instance.task_generator(wait_time=15, sqs_timeout=10)

        self.assertEqual(("task_name", {"foo": "bar"}, second_mock_message), next(gen))

        mock_notify.assert_called_once_with(
            InstanceOf(KeyError),
            context={"body": '{"wrong_key": "task_name", "kwargs": {"foo": "bar"}}'},
        )
        mock_log.exception.assert_called_once_with(
            "failed to get required field %s from content %s with %s",
            InstanceOf(KeyError),
            '{"wrong_key": "task_name", "kwargs": {"foo": "bar"}}',
            {},
        )

        mock_resource.return_value.Queue.return_value.receive_messages.assert_called_with(
            AttributeNames=["All"],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=15,
            VisibilityTimeout=10,
        )
