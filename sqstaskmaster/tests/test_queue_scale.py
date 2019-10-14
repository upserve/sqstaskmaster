import unittest
from unittest.mock import patch, Mock

from botocore.exceptions import ClientError

from sqstaskmaster import queue_scale


@patch("sqstaskmaster.queue_scale.boto3")
class TestBatchJobResourceMonitorTask(unittest.TestCase):
    RULE = {
        queue_scale.Provisioner.QUEUE_NAME: "https://sqs.us-east-1.amazonaws.com/123account_id789/SomeQueueName",
        queue_scale.Provisioner.SERVICE_NAME: "some_ecs_service_name",
        queue_scale.Provisioner.CLUSTER_NAME: "soem_ecs_cluster_name",
        queue_scale.Provisioner.ACTIVE_SIZE: 8,
    }

    def test___init__(self, boto3):
        provisioner = queue_scale.Provisioner([self.RULE])

        boto3.resource.assert_called_with("sqs")
        boto3.client.assert_called_with("ecs")
        self.assertEqual(provisioner.sqs, boto3.resource.return_value)
        self.assertEqual(provisioner.ecs, boto3.client.return_value)

        with self.assertRaisesRegex(TypeError, "Rules must be iterable: 55"):
            queue_scale.Provisioner(55)

        with self.assertRaisesRegex(TypeError, "Each rule must be a dictionary: foo"):
            queue_scale.Provisioner(["foo"])

        with self.assertRaisesRegex(KeyError, "Rules must specify the queue_name"):
            queue_scale.Provisioner([{}])

        with self.assertRaisesRegex(KeyError, "Rules must specify the service_name"):
            queue_scale.Provisioner([{"queue_name": "queue2"}])

        with self.assertRaisesRegex(KeyError, "Rules must specify the cluster_name"):
            queue_scale.Provisioner(
                [{"queue_name": "queue2", "service_name": "service1"}]
            )

        with self.assertRaisesRegex(KeyError, "Rules must specify the active_size"):
            queue_scale.Provisioner(
                [
                    {
                        "queue_name": "queue2",
                        "service_name": "service1",
                        "cluster_name": "cluster3",
                    }
                ]
            )

    def test_run_with_messages(self, boto3):
        provisioner = queue_scale.Provisioner([self.RULE])

        boto3.resource.return_value.Queue.return_value.attributes = {
            name: "2" for name in provisioner.QUEUE_DEPTH_ATTRIBUTES.values()
        }
        provisioner.run()

        boto3.resource.return_value.Queue.assert_called_with(
            self.RULE[provisioner.QUEUE_NAME]
        )
        boto3.client.return_value.update_service.assert_called_with(
            cluster=self.RULE[provisioner.CLUSTER_NAME],
            service=self.RULE[provisioner.SERVICE_NAME],
            desiredCount=self.RULE[provisioner.ACTIVE_SIZE],
        )

    def test_run_without_messages(self, boto3):

        provisioner = queue_scale.Provisioner([self.RULE])
        boto3.resource.return_value.Queue.return_value.attributes = {
            name: "0" for name in provisioner.QUEUE_DEPTH_ATTRIBUTES.values()
        }
        provisioner.run()

        boto3.resource.return_value.Queue.assert_called_with(
            self.RULE[provisioner.QUEUE_NAME]
        )
        boto3.client.return_value.update_service.assert_called_with(
            cluster=self.RULE[provisioner.CLUSTER_NAME],
            service=self.RULE[provisioner.SERVICE_NAME],
            desiredCount=0,
        )

    @patch("sqstaskmaster.queue_scale.logger")
    def test_run_with_client_error(self, log, boto3):
        notify = Mock()
        ce = ClientError({}, "operation")
        provisioner = queue_scale.Provisioner([self.RULE], notify=notify)
        boto3.resource.return_value.Queue.side_effect = Mock(side_effect=ce)
        provisioner.run()

        log.exception.assert_called_once_with(
            "Failed to update resources for rule %s", self.RULE
        )
        notify.assert_called_once_with(ce, context=self.RULE)
