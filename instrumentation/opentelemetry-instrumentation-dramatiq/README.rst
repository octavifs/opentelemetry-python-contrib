OpenTelemetry dramatiq Instrumentation
===========================

|pypi|

.. |pypi| image:: https://badge.fury.io/py/opentelemetry-instrumentation-dramatiq.svg
   :target: https://pypi.org/project/opentelemetry-instrumentation-dramatiq/

This library allows tracing requests made by the dramatiq library.

Installation
------------


::

    pip install opentelemetry-instrumentation-dramatiq

Usage
-----
For now, auto instrumentation will likely cause issues, so if you use *opentelemetry-instrument* for auto instrumentation, make sure you disable it for dramatiq. Set in your environment variables:

::
    
    OTEL_PYTHON_DISABLED_INSTRUMENTATIONS=dramatiq

To instrument a broker, add the following code to your application, right after the broker has been initialized:

::
    
    from opentelemetry.instrumentation.dramatiq import DramatiqInstrumentor

    DramatiqInstrumentor().instrument(broker=redis_broker)

Haven't tested so far RabbitMQ backend, although it would probably work. The current implementation covers both traces and metrics. Attributes are prefixed with *dramatiq.*.

References
----------

* `OpenTelemetry dramatiq/ Tracing <https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/dramatiq/dramatiq.html>`_
* `OpenTelemetry Project <https://opentelemetry.io/>`_
