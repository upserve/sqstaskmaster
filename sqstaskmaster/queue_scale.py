import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class Provisioner:
    """
    Runnable Task to monitor SQS queue depth and adjust the number of ECS service works.
    Based on declarative rules for queues, services and clusters.
    Current implementation provides only an all on, or all off approach, setting the number of works to ACTIVE_SIZE or
    zero. Tasks should be evenly distributed between workers assuming the number of workers is much smaller than the
    number of tasks. The tail as workers run out of work tasks is ignored.

    Usage:
    RULES = [
        {
            QUEUE_NAME: "{SQS_URL}",
            SERVICE_NAME: "{ECS_SERVICE_NAME}",
            CLUSTER_NAME: "{ECS_CLUSTER_NAME}",
            ACTIVE_SIZE: N
        }
    ]

    provisioner = Provisioner(Rules)
    while True:
        provisioner.run()
        time.sleep(10)

    Or use a more complete scheduler like https://pypi.org/project/schedule/
    """

    QUEUE_NAME = "queue_name"
    SERVICE_NAME = "service_name"
    CLUSTER_NAME = "cluster_name"
    ACTIVE_SIZE = "active_size"

    QUEUE_DEPTH_ATTRIBUTES = {
        "pending_jobs": "ApproximateNumberOfMessages",
        "running_jobs": "ApproximateNumberOfMessagesNotVisible",
        "scheduled_jobs": "ApproximateNumberOfMessagesDelayed",
    }

    def __init__(self, rules, notify=None):
        self._rules = rules

        if not hasattr(rules, "__iter__"):
            raise TypeError("Rules must be iterable: {}".format(rules))
        for rule in rules:
            if not isinstance(rule, dict):
                raise TypeError("Each rule must be a dictionary: {}".format(rule))

            if self.QUEUE_NAME not in rule:
                raise KeyError("Rules must specify the {}".format(self.QUEUE_NAME))

            if self.SERVICE_NAME not in rule:
                raise KeyError("Rules must specify the {}".format(self.SERVICE_NAME))

            if self.CLUSTER_NAME not in rule:
                raise KeyError("Rules must specify the {}".format(self.CLUSTER_NAME))

            if self.ACTIVE_SIZE not in rule:
                raise KeyError("Rules must specify the {}".format(self.ACTIVE_SIZE))

        self._notify = notify

        self.sqs = boto3.resource("sqs")
        self.ecs = boto3.client("ecs")

    def run(self):
        """
        For each rule, check the queue depth and adjust the service count
        """
        logger.info("Checking queue for work and scaling service resources")

        for rule in self._rules:
            logger.debug("running rule: %s", rule)
            try:
                queue_attributes = self.sqs.Queue(rule[self.QUEUE_NAME]).attributes
                total_messages = sum(
                    int(queue_attributes[val])
                    for val in self.QUEUE_DEPTH_ATTRIBUTES.values()
                )

                # This could result in thrashing the service count. For tasks that run quickly some historical filter
                # might be required
                self.ecs.update_service(
                    cluster=rule[self.CLUSTER_NAME],
                    service=rule[self.SERVICE_NAME],
                    desiredCount=rule[self.ACTIVE_SIZE] if total_messages > 0 else 0,
                )

                logger.info(
                    "Setting: %s %s to %s count",
                    rule[self.CLUSTER_NAME],
                    rule[self.SERVICE_NAME],
                    rule[self.ACTIVE_SIZE] if total_messages > 0 else 0,
                )

                self.log_queue_depth(queue_attributes, rule)

            except ClientError as ce:
                logger.exception("Failed to update resources for rule %s", rule)
                self.notify(ce, context=rule)

    def log_queue_depth(self, queue_attributes, rule):
        for name, attr in self.QUEUE_DEPTH_ATTRIBUTES.items():
            logger.info(
                "queue_health_monitor: name - %s; %s - %s",
                name,
                queue_attributes[attr],
                rule[self.QUEUE_NAME],
            )

    def notify(self, exception, context=None):
        if self._notify:
            self._notify(exception, context=context)
