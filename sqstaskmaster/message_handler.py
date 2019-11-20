import logging
import signal
import time

from abc import ABC, abstractmethod
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class MessageHandler(ABC):
    """
    MessageHandler is an abstract class used to implement safe message handling.
    The implementer must provide run and running methods.

    The handler is then used as a context manager to manage the process execution and ack the result in SQS
    If the Run process does not raise it is assumed to have completed

    Currently the handler will catch any Exception and log + notify allowing the worker to continue to the next message
    The SQS message will not be ack'd (deleted) if an exception is raised.

    Any BaseException (not Exception) will be reraised by __exit__ allowing the python process to exit
    """

    def __init__(self, message, sqs_timeout, alarm_timeout, hard_timeout):
        """
        Constructor for message processing context manager

        :param message: the SQS message
        :param sqs_timeout: the timeout to set when the process is still working on the message
        :param alarm_timeout: the timeout to use on the local system to check if running and update SQS
        :param hard_timeout: The hard limit for executing the message processing
        """
        self._message = message
        self.sqs_timeout = sqs_timeout
        self.alarm_timeout = alarm_timeout
        self.hard_timeout = hard_timeout

        if alarm_timeout <= 0:
            raise ValueError("Alarm timeout must be an integer greater than zero")

        if sqs_timeout <= 0:
            raise ValueError("SQS timeout must be an integer greater than zero")

        if hard_timeout <= 0:
            raise ValueError("Hard timeout must be an integer greater than zero")

        if (alarm_timeout + 1) >= sqs_timeout:
            raise ValueError(
                "Alarm timeout {:d} is to long to reset the sqs timeout {:d}".format(
                    alarm_timeout, sqs_timeout
                )
            )

        self._start_time = time.time()

    def __enter__(self):
        signal.signal(signal.SIGALRM, self._handle_alarm)
        signal.alarm(self.alarm_timeout)
        self._extend_timeout(self.sqs_timeout)
        return self

    def _run_time(self):
        return time.time() - self._start_time

    def _handle_alarm(self, signum, frame):
        logger.info("Handling %s for %s in frame %s", signum, self, frame)
        if self._run_time() > self.hard_timeout:
            raise TimeoutError("Hit Hard Timeout for message handler")
        elif self.running():
            logger.info("Adjusting sqs timeout")
            try:
                self._extend_timeout(self.sqs_timeout)
                signal.alarm(self.alarm_timeout)
            except ClientError as ce:
                # Don't fail here - report and continue - will result in running the task many times
                self.notify(
                    ce, context={"body": self._message.body, **self._message.attributes}
                )
                logger.exception("Failed to extend timeout for %s", self)
        else:
            raise RuntimeError("Handler is stuck on %s", self)

    def _extend_timeout(self, seconds):
        self._message.change_visibility(VisibilityTimeout=seconds)

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)
        if exc_type is None:
            try:
                self._message.delete()
            except ClientError as ce:
                self.notify(
                    ce, context={"body": self._message.body, **self._message.attributes}
                )
                logger.exception("failed to delete message %s", self._message)

        else:
            # Should the message visibility time be set to 0 here?
            self.notify(
                exc_val,
                context={"body": self._message.body, **self._message.attributes},
            )
            logger.exception("Failed for message: %s", self._message)

        # catch only Exception not BaseException
        # https://docs.python.org/3/library/exceptions.html#exception-hierarchy
        return isinstance(exc_val, Exception)

    @abstractmethod
    def notify(self, exception, context=None):
        """
        Provide a method for notification of errors outside the logger such as HoneyBadger
        :param exception: the exception to notify about
        :param context: the context in which it occurred
        """

    def __str__(self):
        return "MessageHandler: sqs {}, alarm {}, run{}, hard {}, content {}, attrs {}".format(
            self.sqs_timeout,
            self.alarm_timeout,
            self._run_time(),
            self.hard_timeout,
            self._message.body,
            self._message.attributes,
        )

    @abstractmethod
    def running(self):
        """
        Detect whether the handler process is stuck
        :return: True if the process handler is still executing. False if the handler process is stopped or stuck
        """
        pass

    @abstractmethod
    def run(self):
        """
        Tasks should implement a single method with no arguments to execute the task.
        Instance attributes should be created to allow the running method to introspect the progress as needed.
        :return: execute the task
        """
        pass
