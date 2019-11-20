SQS Task Master for Python
==================================

.. image:: https://travis-ci.org/Automatic/taskhawk-python.svg?branch=master
    :target: https://travis-ci.org/upserve/sqstaskmaster

.. image:: https://img.shields.io/pypi/v/sqstaskmaster.svg?style=flat-square
    :target: https://pypi.python.org/pypi/sqstaskmaster

.. image:: https://img.shields.io/pypi/pyversions/sqstaskmaster.svg?style=flat-square
    :target: https://pypi.python.org/pypi/sqstaskmaster

.. image:: https://img.shields.io/pypi/implementation/sqstaskmaster.svg?style=flat-square
    :target: https://pypi.python.org/pypi/sqstaskmaster

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/upserve/sqstaskmaster


**SQS Task Master** is one of many packages for distributed asynchronous task execution in python.
This package is different in that it is designed to work with uncooperative tasks.
It is designed for CPU intensive tasks which will not work in a python cooperative threading model.
It allows for tasks that may take upto 12 hours (the limit for SQS).
It also provides for applying a hard timeout for a task, less than 12 hours.
The implementation relies on using os Singals to achieve these behaviors.
Retries and failures are handled using SQS infrastructure.
It is designed to work with standard SQS queues which guarantee at least once delivery.

Upserve developed this library for use with long running data science tools like
`xgBoost <https://github.com/dmlc/xgboost/tree/master/python-package>`_ and
`OrTools <https://github.com/google/or-tools>`_.
While python `signal <https://docs.python.org/3/library/signal.html#execution-of-python-signal-handlers>`_
will attempt to interrupt the process, you should verify the desired behavior.
This behavior and shortcomings with c libraries is documented in TestMessageHandler.test_cbusy_timeout.


Other choices:
 - https://github.com/Automatic/taskhawk-python
 - https://github.com/mardix/sqs-task
 - https://github.com/celery/celery

Installation
************

The sqstaskmaster project is public and can be installed from pypi with pip for python3 on mac and linux.

::

  pip install sqstaskmaster

Usage
*****

Publish Tasks
::

  tm = TaskManager(sqs_url)
  tm.submit('MyTask', some='kwarg', someother='kwarg')
  tm.submit('MyOtherTask', **kwargs)

Create a Handler
::

  class MyHandler(MessageHandler):
    def __init__(self, message, sqs_timeout, alarm_timeout, hard_timeout=4 * 60 * 60, **kwargs):
        super().__init__(message, sqs_timeout, alarm_timeout, hard_timeout)
        self.kwargs = kwargs
        # prefer parsing arguments (for instance dates) in the consume task and passing explicit arguments

    def running(self):
        # If you can inspect the task to see if it is hung, do so here
        return True

    def run(self):
        # Do some work here!

    def notify(self, exception, context=None):
        # Implement only for error notification service. Errors are already logged.
        pass

Consume Tasks
::

  for task, kwargs, message in TaskManager(sqs_url).task_generator(sqs_timeout=30):
    if task == 'MyTask':
      # prefer parsing kwargs here and handle KeyError and ValueError appropriately
      with MyHandler(message, sqs_timeout=30, alarm_timeout=25, hard_timeout=300, **kwargs) as handler:
        handler.run()
    elif task == 'MyOtherTask':
      with MyOtherHandler(message, sqs_timeout=30, alarm_timeout=25, hard_timeout=300, **kwargs) as handler:
        handler.run()
    else:
      # Do something about unexpected task request


Local Integration Testing

Testing your production system should include a combination of: local stubbing using Mock; tools like
`localstack <https://github.com/localstack/localstack>`_ or a staging environment to examine behavior in a distributed
system; and purely local validation of consumer / producer api. LocalQueue is a pure in memory solution that implements
a minimal subset of the SQS API for this last purpose only. The intent here is not to test or validate behavior of
consumers and producers interacting with SQS, but only the message content. LocalQueue could be extended to implement
threading and behave more like sqs within a single process, but that is better done with another tool like local stack.

::

  ...
  tm = TaskManager(url, queue_constructor=LocalQueue)
  ...

By passing the queue_constructor argument to the TaskManager, you can bypass AWS SQS and use a local, in memory queue
object to send and receive messages allowing local synchronous integration testing. This creates one global queue for
each url. It is not for testing system behavior and does not implement retries, message timeouts or other SQS features.
It is only for integration testing of the interface between your producer and consumer methods with the TaskManager
serialization and MessageHandler execution.

Development
***********

Git clone the respository:
::

  git clone git@github.com:upserve/sqstaskmaster.git

Pip install the development dependencies in a `virtual environment <https://virtualenvwrapper.readthedocs.io/en/latest/>`_:
::

  pip install -e .[dev]

Run unit tests:
::

  python -m unittest -v

Run flake8:
::

  flake8 .

Run black:
::

  black .