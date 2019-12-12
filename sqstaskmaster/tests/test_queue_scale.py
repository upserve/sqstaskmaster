import unittest
from unittest.mock import patch, Mock

from botocore.exceptions import ClientError

from sqstaskmaster import queue_scale
from sqstaskmaster.queue_scale import ServiceState


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

        with patch.object(
            queue_scale.Provisioner, "service_state", lambda _, __: ServiceState.ACTIVE
        ):
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
        with patch.object(
            queue_scale.Provisioner, "service_state", lambda _, __: ServiceState.ACTIVE
        ):
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
        with patch.object(
            queue_scale.Provisioner, "service_state", lambda _, __: ServiceState.ACTIVE
        ):
            provisioner.run()
        log.exception.assert_called_once_with(
            "Failed to update resources for rule %s", self.RULE
        )
        notify.assert_called_once_with(ce, context=self.RULE)

    def test_run_with_status_not_active(self, boto3):
        provisioner = queue_scale.Provisioner([self.RULE])
        with patch.object(
            queue_scale.Provisioner,
            "service_state",
            lambda _, __: ServiceState.STARTING,
        ):
            provisioner.run()
        boto3.client.return_value.update_service.assert_not_called()

    def test_get_description_success(self, boto3):
        provisioner = queue_scale.Provisioner([self.RULE])

        mock_result = Mock()
        boto3.client.return_value.describe_services.return_value = {
            "services": [mock_result],
            "failures": [],
        }

        self.assertEqual(mock_result, provisioner.get_description(self.RULE))
        boto3.client.return_value.describe_services.assert_called_once_with(
            cluster=self.RULE[provisioner.CLUSTER_NAME],
            services=[self.RULE[provisioner.SERVICE_NAME]],
            include=["TAGS"],
        )

    def test_get_description_failure(self, boto3):
        provisioner = queue_scale.Provisioner([self.RULE])

        boto3.client.return_value.describe_services.return_value = {
            "services": [],
            "failures": [{"arn": "some_arn", "reason": "MISSING"}],
        }

        self.assertDictEqual({}, provisioner.get_description(self.RULE))
        boto3.client.return_value.describe_services.assert_called_once_with(
            cluster=self.RULE[provisioner.CLUSTER_NAME],
            services=[self.RULE[provisioner.SERVICE_NAME]],
            include=["TAGS"],
        )

    def test_list_tags_success(self, boto3):
        provisioner = queue_scale.Provisioner([self.RULE])
        boto3.client.return_value.list_tags_for_resource.return_value = {
            "tags": [{"key": "string", "value": "string"}]
        }
        self.assertListEqual(
            [{"key": "string", "value": "string"}], provisioner.get_tags("foo_arn")
        )
        boto3.client.return_value.list_tags_for_resource.assert_called_once_with(
            resourceArn="foo_arn"
        )

    def test_list_tags_failure(self, boto3):
        """
        Example of actual Error:
        InvalidParameterException: An error occurred (InvalidParameterException) when calling the ListTagsForResource
        operation: The service with name {YOUR BAD ARN} cannot be found. Specify a valid service name and try again.

        InvalidParameterException inherits from ClientError, but can not be directly imported for testing / handling.
        """
        provisioner = queue_scale.Provisioner([self.RULE])
        ce = ClientError(
            {
                "Error": {
                    "Code": "InvalidParameterException",
                    "Message": "Stuff happens...",
                }
            },
            "ListTagsForResource",
        )
        boto3.client.return_value.list_tags_for_resource.side_effect = ce
        with self.assertRaisesRegex(
            ClientError,
            r"An error occurred \(InvalidParameterException\) when calling the ListTagsForResource operation: "
            r"Stuff happens...",
        ):
            provisioner.get_tags("foo_arn")
