import signal
import time
import unittest
import builtins
import subprocess
import pyximport

from abc import ABC
from unittest.mock import patch, Mock, DEFAULT
from botocore.exceptions import ClientError
from sqstaskmaster.message_handler import MessageHandler

pyximport.install(language_level=3)
from sqstaskmaster.tests import cbusy  # noqa: ignore=E402


class TestHandler(MessageHandler, ABC):
    def notify(self, exception, context=None):
        pass

    def running(self):
        return True


class TestBusyHandler(TestHandler):
    def run(self):
        while True:
            pass


class TestSleepHandler(TestHandler):
    def run(self):
        while True:
            time.sleep(30)


class TestProcessHandler(TestHandler):
    def run(self):
        subprocess.call("while true; do true; done", shell=True)


class TestCBuysHandler(TestHandler):
    def run(self):
        cbusy.busy()


@patch.multiple(MessageHandler, __abstractmethods__=set(), notify=DEFAULT)
class TestMessageHandler(unittest.TestCase):
    @patch("time.time")
    def test___init__(self, mock_time, **kwargs):
        mock_message = Mock()
        instance = MessageHandler(
            mock_message, sqs_timeout=13, alarm_timeout=11, hard_timeout=17
        )

        self.assertEqual(instance._start_time, mock_time.return_value)
        self.assertEqual(instance.sqs_timeout, 13)
        self.assertEqual(instance.alarm_timeout, 11)
        self.assertEqual(instance.hard_timeout, 17)

        with self.assertRaisesRegex(
            ValueError, "Alarm timeout 12 is to long to reset the sqs timeout 13"
        ):
            MessageHandler(
                mock_message, sqs_timeout=13, alarm_timeout=12, hard_timeout=17
            )

        with self.assertRaisesRegex(
            ValueError, "SQS timeout must be an integer greater than zero"
        ):
            MessageHandler(
                mock_message, sqs_timeout=0, alarm_timeout=12, hard_timeout=17
            )

        with self.assertRaisesRegex(
            ValueError, "Alarm timeout must be an integer greater than zero"
        ):
            MessageHandler(
                mock_message, sqs_timeout=13, alarm_timeout=0, hard_timeout=17
            )

        with self.assertRaisesRegex(
            ValueError, "Hard timeout must be an integer greater than zero"
        ):
            MessageHandler(
                mock_message, sqs_timeout=13, alarm_timeout=12, hard_timeout=0
            )

    @patch("time.time")
    def test__runtime(self, mock_time, **kwargs):
        mock_message = Mock()
        instance = MessageHandler(
            mock_message, sqs_timeout=13, alarm_timeout=11, hard_timeout=17
        )

        # mock_time is a mock object and the difference of the two return values mock the formula, not a constant value
        self.assertEqual(
            instance._run_time(), mock_time.return_value - mock_time.return_value
        )

    @patch("signal.alarm")
    @patch("signal.signal")
    def test____enter__(self, mock_signal, mock_alarm, **kwargs):
        mock_message = Mock()
        instance = MessageHandler(
            mock_message, sqs_timeout=13, alarm_timeout=11, hard_timeout=17
        )

        instance.__enter__()
        mock_alarm.assert_called_once_with(11)
        mock_signal.assert_called_once_with(signal.SIGALRM, instance._handle_alarm)

    @patch("signal.alarm")
    def test___exit__no_error(self, mock_alarm, **kwargs):
        mock_message = Mock()
        instance = MessageHandler(
            mock_message, sqs_timeout=13, alarm_timeout=11, hard_timeout=17
        )

        self.assertTrue(
            "should always return true", instance.__exit__(None, None, None)
        )
        mock_alarm.assert_called_once_with(0)
        mock_message.delete.assert_called_once_with()

    @patch("signal.alarm")
    @patch("sqstaskmaster.message_handler.logger")
    def test___exit__delete_fails(self, mock_logger, mock_alarm, notify=None):
        mock_message = Mock()
        instance = MessageHandler(
            mock_message, sqs_timeout=13, alarm_timeout=11, hard_timeout=17
        )

        ce = ClientError({}, "operation")
        mock_message.delete.side_effect = ce
        mock_message.attributes.keys.return_value = []
        self.assertTrue(
            "should always return true", instance.__exit__(None, None, None)
        )
        mock_alarm.assert_called_once_with(0)
        mock_logger.exception.assert_called_once_with(
            "failed to delete message %s", mock_message
        )
        notify.assert_called_once_with(
            ce, context={"body": mock_message.body, **mock_message.attributes}
        )

    @patch("signal.alarm")
    @patch("sqstaskmaster.message_handler.logger")
    def test___exit__exception_raised(self, mock_logger, mock_alarm, notify=None):
        mock_message = Mock()
        instance = MessageHandler(
            mock_message, sqs_timeout=13, alarm_timeout=11, hard_timeout=17
        )

        exc_type, exc_val, exc_tb = Mock(), Mock(), Mock()

        mock_message.attributes.keys.return_value = []
        self.assertTrue(
            "should always return true", instance.__exit__(exc_type, exc_val, exc_tb)
        )

        mock_message.delete.assert_not_called()
        mock_alarm.assert_called_once_with(0)
        mock_logger.exception.assert_called_once_with(
            "Failed for message: %s", mock_message
        )
        notify.assert_called_once_with(
            exc_val, context={"body": mock_message.body, **mock_message.attributes}
        )

    @patch("signal.alarm")
    def test__handle_alarm(self, mock_signal_alarm, **kwargs):
        mock_message = Mock()
        instance = MessageHandler(
            mock_message, sqs_timeout=13, alarm_timeout=11, hard_timeout=17
        )

        signum = Mock()
        frame = Mock()

        with patch.object(instance, "running", return_value=True) as running:
            # Happy path
            with patch.object(instance, "_run_time", return_value=16):
                instance._handle_alarm(signum, frame)
                mock_signal_alarm.assert_called_with(11)
                mock_message.message.change_visibility.called_once_with(
                    VisibilityTimeout=instance.sqs_timeout
                )

                # if running is false
                running.return_value = True
                with self.assertRaisesRegex(RuntimeError, "Handler is stuck on"):
                    instance.running.return_value = False
                    instance._handle_alarm(signum, frame)

            # if runtime exceeded
            with patch.object(instance, "_run_time", return_value=18):
                with self.assertRaisesRegex(
                    TimeoutError, "Hit Hard Timeout for message handler"
                ):
                    instance._handle_alarm(signum, frame)

    @patch("signal.alarm")
    @patch("signal.signal")
    def test_handler_success(self, mock_signal, mock_alarm, **kwargs):
        mock_message = Mock()
        with MessageHandler(
            mock_message, sqs_timeout=3, alarm_timeout=1, hard_timeout=1
        ) as instance:
            mock_alarm.assert_called_once_with(1)
            mock_signal.assert_called_once_with(signal.SIGALRM, instance._handle_alarm)
            mock_alarm.reset_mock()

        mock_message.delete.assert_called_once_with()
        mock_alarm.assert_called_once_with(0)

    @patch("signal.alarm")
    @patch("signal.signal")
    @patch("sqstaskmaster.message_handler.logger")
    def test_handler_failure_exception(
        self, mock_log, mock_signal, mock_alarm, notify=None
    ):
        mock_message = Mock()
        mock_message.attributes.keys.return_value = []
        error = Exception("FooBar")
        with MessageHandler(
            mock_message, sqs_timeout=3, alarm_timeout=1, hard_timeout=1
        ) as instance:
            mock_alarm.assert_called_once_with(1)
            mock_signal.assert_called_once_with(signal.SIGALRM, instance._handle_alarm)
            mock_alarm.reset_mock()
            raise error

        mock_message.delete.assert_not_called()
        mock_alarm.assert_called_once_with(0)

        mock_log.exception.assert_called_once_with(
            "Failed for message: %s", mock_message
        )
        notify.assert_called_once_with(
            error, context={"body": mock_message.body, **mock_message.attributes}
        )

    @patch("signal.alarm")
    @patch("signal.signal")
    @patch("sqstaskmaster.message_handler.logger")
    def test_handler_fail_base_exception(
        self, mock_log, mock_signal, mock_alarm, notify=None
    ):
        mock_message = Mock()
        mock_message.attributes.keys.return_value = []
        error = BaseException("FooBar")
        with self.assertRaises(BaseException):
            with MessageHandler(
                mock_message, sqs_timeout=3, alarm_timeout=1, hard_timeout=1
            ) as instance:
                mock_alarm.assert_called_once_with(1)
                mock_signal.assert_called_once_with(
                    signal.SIGALRM, instance._handle_alarm
                )
                mock_alarm.reset_mock()
                raise error

        mock_message.delete.assert_not_called()
        mock_alarm.assert_called_once_with(0)

        mock_log.exception.assert_called_once_with(
            "Failed for message: %s", mock_message
        )
        notify.assert_called_once_with(
            error, context={"body": mock_message.body, **mock_message.attributes}
        )

    @patch("signal.alarm")
    @patch("signal.signal")
    @patch("sqstaskmaster.message_handler.logger")
    def test_handler_timeout(self, mock_log, mock_signal, mock_alarm, notify=None):
        mock_message = Mock()
        mock_message.attributes.keys.return_value = []
        error = TimeoutError("foobar")

        with patch.object(builtins, "TimeoutError", return_value=error) as mock_timeout:
            with MessageHandler(
                mock_message, sqs_timeout=3, alarm_timeout=1, hard_timeout=1
            ) as instance:
                mock_alarm.assert_called_once_with(1)
                mock_signal.assert_called_once_with(
                    signal.SIGALRM, instance._handle_alarm
                )
                mock_alarm.reset_mock()
                instance._start_time = time.time() - 30
                instance._handle_alarm(Mock(), Mock())

        mock_timeout.assert_called_once_with("Hit Hard Timeout for message handler")

        mock_message.delete.assert_not_called()
        mock_alarm.assert_called_once_with(0)

        mock_log.exception.assert_called_once_with(
            "Failed for message: %s", mock_message
        )
        notify.assert_called_once_with(
            error, context={"body": mock_message.body, **mock_message.attributes}
        )

    def helper(self, clazz):
        mock_message = Mock()
        mock_message.attributes.keys.return_value = []

        with clazz(
            mock_message, sqs_timeout=3, alarm_timeout=1, hard_timeout=1
        ) as instance:
            with self.assertRaises(TimeoutError):
                instance.run()

    def test_busy_loop_timeout(self, **kwargs):
        self.helper(TestBusyHandler)

    def test_external_process_timeout(self, **kwargs):
        self.helper(TestProcessHandler)

    def test_sleep_timeout(self, **kwargs):
        self.helper(TestSleepHandler)

    def test_cbusy_timeout(self, **kwargs):
        """
        This test takes 2 seconds instead of 1. It passes because the handler is called when the c busy loop exits,
        but the tight loop in c is not interrupted at 1 second
        :param kwargs: not used
        """
        self.helper(TestCBuysHandler)
