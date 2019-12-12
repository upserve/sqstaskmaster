import logging
import boto3
import enum
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class ServiceState(enum.Enum):
    STARTING = enum.auto()
    ACTIVE = enum.auto()
    STOPPING = enum.auto()
    FAILING = enum.auto()


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
                # might be required but that is not the intended use case.
                # Beware of attempting to set the count based on the number of messages in the queue. It will be
                # difficult to control which instances get shutdown and ensure a graceful exit.
                # All or nothing is simple and only wasteful at the tail end of a large process queue

                current_state = self.service_state(rule)
                if current_state == ServiceState.ACTIVE:
                    self.ecs.update_service(
                        cluster=rule[self.CLUSTER_NAME],
                        service=rule[self.SERVICE_NAME],
                        desiredCount=rule[self.ACTIVE_SIZE]
                        if total_messages > 0
                        else 0,
                    )

                    logger.info(
                        "Setting: %s %s to %s count",
                        rule[self.CLUSTER_NAME],
                        rule[self.SERVICE_NAME],
                        rule[self.ACTIVE_SIZE] if total_messages > 0 else 0,
                    )
                else:
                    logger.info(
                        "Service: %s %s is in state: %s; Desired count will not be adjusted",
                        rule[self.CLUSTER_NAME],
                        rule[self.SERVICE_NAME],
                        current_state,
                    )

                self.log_queue_depth(queue_attributes, rule)

            except ClientError as ce:
                logger.exception("Failed to update resources for rule %s", rule)
                self.notify(ce, context=rule)

    def get_description(self, rule):
        """
        Get the description of the service and its deployments - can be large
        An unknown resource will result in an empty response.

        The api specifies the ability to get the tags, but by observation they are not returned even when requested.
        Use get_tags.
        """
        result = self.ecs.describe_services(
            cluster=rule[self.CLUSTER_NAME],
            services=[rule[self.SERVICE_NAME]],
            include=[
                "TAGS"
            ],  # appears to ignore this field - response does not have a tags field
        )

        if result["failures"]:
            logger.error("ECS Describe Services Failed: %s", result["failures"])
            return {}
        return result["services"][0]

    def get_tags(self, arn):
        """
        Get the tags associated with a resource, the describe_services api does not appear to actually return them
        when requested.

        Throws InvalidParameterException for an bad ARN. Use 'except ClientError:' InvalidParameterException is not
        importable
        """
        return self.ecs.list_tags_for_resource(resourceArn=arn)["tags"]

    def service_state(self, _rule):
        """
        :param _rule: the rule to check the service state on

        Override this method to manage service state. Check a tag or other service status to determine whether the
        provisioner should attempt to modify the desiredCount. This prevents race conditions during service deployment.
        """
        logger.warning(
            "Provisioner using default 'always active' service_state implementation"
        )
        # Typically the application would check a TAG on the service description to determine state.
        # Checking a tag and then setting the count is not atomic and does not guarantee race free operation.
        return ServiceState.ACTIVE

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
