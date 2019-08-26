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
`XgBoost <https://github.com/dmlc/xgboost/tree/master/python-package>`_ and
`OrTools <https://github.com/google/or-tools>`_.
While python `singal <https://docs.python.org/3/library/signal.html#execution-of-python-signal-handlers>`_
will attempt to interrupt the process, you should verify the desired behavior.
This behavior is documented in TestMessageHandler.test_cbusy_timeout.


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

  foo


Consume Tasks
::

  for task, kwargs, message in TaskManager(sqs_url).task_generator(sqs_timeout=30):
    if task == 'MyTask':
      with MyHandler(message, sqs_timeout=30, alarm_timeout=25, hard_timeout=300, **kwargs) as handler:
        handler.run()
    elif task == 'MyOtherTask':
      with MyOtherHandler(message, sqs_timeout=30, alarm_timeout=25, hard_timeout=300, **kwargs) as handler:
        handler.run()
    else:
      # Do something about unexpected task request


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