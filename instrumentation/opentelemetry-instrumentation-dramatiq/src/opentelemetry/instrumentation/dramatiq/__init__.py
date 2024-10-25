# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Usage
-----

Start broker backend

::

    docker run -p 6379:6379 redis

Run instrumented actor

.. code-block:: python

    import dramatiq
    from dramatiq.brokers.redis import RedisBroker
    from opentelemetry.instrumentation.dramatiq import DramatiqInstrumentor


    broker = RedisBroker()
    DramatiqInstrumentor().instrument(broker=broker)

    @dramatiq.actor
    def multiply(x, y):
        return x * y

    broker.declare_actor(count_words)

    multiply.send(43, 51)

"""

import os
import threading
import traceback
from time import time
from typing import Any, Collection, Dict, Literal

import dramatiq
from opentelemetry import baggage, context, trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.metrics import get_meter_provider
from opentelemetry.propagate import extract, inject
from opentelemetry.util.types import Attributes

from . import utils
from .package import _instruments
from .version import __version__

_DramatiqBrokerActionType = Literal["send", "process"]

_DRAMATIQ_MESSAGE_ACTION_KEY = "dramatiq.action"

_DRAMATIQ_MESSAGE_ACTOR_KEY = "dramatiq.actor_name"
_DRAMATIQ_MESSAGE_QUEUE_KEY = "dramatiq.queue_name"
_DRAMATIQ_MESSAGE_ID_KEY = "dramatiq.message_id"
_DRAMATIQ_MESSAGE_REDIS_ID_KEY = "dramatiq.message_redis_id"

_DRAMATIQ_MESSAGE_RETRY_COUNT_KEY = "dramatiq.retry_count"

_DRAMATIQ_MESSAGE_PROCESS_TIME_KEY = "opentelemetry_metrics.process_time"
_DRAMATIQ_MESSAGE_ENQUEUED_TIME_KEY = "opentelemetry_metrics.enqueue_time"
_DRAMATIQ_MESSAGE_DELAYED_TIME_KEY = "opentelemetry_metrics.delayed_time"


def _current_millis():
    """Returns the current UNIX time in milliseconds."""
    return int(time() * 1000)


class _InstrumentationMiddleware(dramatiq.Middleware):
    def __init__(self, _tracer: trace.Tracer):
        self._tracer: trace.Tracer = _tracer
        self._span_registry = {}

        meter = get_meter_provider().get_meter(
            "opentelemetry.instrumentation.dramatiq", __version__
        )

        # Total counts. Please be aware that its a "total" per PROCESS and THREAD, NOT GLOBAL.
        # All metrics emitted have a dramatiq.process.pid and dramatiq.process.thread_id attribute, so you can
        # know which process and thread emitted the metric, and ultimately aggregate them, to compute a global view.
        self.broker_processed_total = meter.create_counter(
            "dramatiq.messages.processed.total_count",
            description="Total number of messages processed since the broker started. Accounts for failed messages.",
        )
        self.broker_retried_total = meter.create_counter(
            "dramatiq.messages.retried.total_count",
            description="Total number of messages processed that are retries since the broker started.",
        )
        self.broker_failed_total = meter.create_counter(
            "dramatiq.messages.failed.total_count",
            description="Total number of processed messages that failed since the broker started.",
        )
        self.broker_queued_total = meter.create_counter(
            "dramatiq.messages.queued.total_count",
            description="Total number of messages queued since the broker started.",
        )
        self.broker_delayed_total = meter.create_counter(
            "dramatiq.messages.delayed.total_count",
            description="Total number of messages delayed since the broker started.",
        )

        # Duration histograms
        self.broker_processed_duration = meter.create_histogram(
            "dramatiq.messages.processed.duration",
            unit="ms",
            description="Milliseconds spent processing.",
        )

        self.broker_queued_duration = meter.create_histogram(
            "dramatiq.messages.queued.duration",
            unit="ms",
            description="Milliseconds spent queued.",
        )

        self.broker_delayed_duration = meter.create_histogram(
            "dramatiq.messages.delayed.duration",
            unit="ms",
            description="Milliseconds spent delayed.",
        )

    def _get_message_attributes(self, message: dramatiq.Message) -> Attributes:
        return {
            _DRAMATIQ_MESSAGE_QUEUE_KEY: message.queue_name,
            _DRAMATIQ_MESSAGE_ACTOR_KEY: message.actor_name,
        }

    def _get_process_attributes(self) -> Attributes:
        return {
            "dramatiq.process.pid": os.getpid(),
            "dramatiq.process.thread_id": threading.get_native_id(),
        }

    def _get_span_attributes(
        self, action_type: _DramatiqBrokerActionType, message: dramatiq.Message
    ) -> Dict[str, Any]:
        span_attributes = {
            _DRAMATIQ_MESSAGE_ACTION_KEY: action_type,
            _DRAMATIQ_MESSAGE_ACTOR_KEY: message.actor_name,
            _DRAMATIQ_MESSAGE_QUEUE_KEY: message.queue_name,
            _DRAMATIQ_MESSAGE_ID_KEY: message.message_id,
        }
        if "redis_message_id" in message.options:
            span_attributes[_DRAMATIQ_MESSAGE_REDIS_ID_KEY] = message.options[
                "redis_message_id"
            ]
        return span_attributes

    def after_nack(self, broker: dramatiq.Broker, message: dramatiq.Message):
        pass

    def before_delay_message(self, broker: dramatiq.Broker, message: dramatiq.Message):
        current_message_attributes = self._get_message_attributes(message)
        current_process_attributes = self._get_process_attributes()
        # Adding up the delayed message count with and without message attributes (actor and queue)
        self.broker_delayed_total.add(
            1, {**current_message_attributes, **current_process_attributes}
        )
        message.options[_DRAMATIQ_MESSAGE_DELAYED_TIME_KEY] = _current_millis()

    def before_process_message(
        self, _broker: dramatiq.Broker, message: dramatiq.Message
    ):
        # Calculate metrics
        retry_count = message.options.get("retries", 0)
        current_message_attributes = self._get_message_attributes(message)
        current_process_attributes = self._get_process_attributes()
        now_millis = _current_millis()
        self.broker_queued_total.add(
            0, {**current_message_attributes, **current_process_attributes}
        )  # this should be 0, as we are processing the message, but will still trigger an update in the metric, so it will appear alongside the processed metric
        self.broker_processed_total.add(
            1, {**current_message_attributes, **current_process_attributes}
        )
        if retry_count > 0:
            self.broker_retried_total.add(
                1, {**current_message_attributes, **current_process_attributes}
            )
        message.options[_DRAMATIQ_MESSAGE_PROCESS_TIME_KEY] = now_millis

        # Remove from queue metrics
        message_enqueue_time = message.options.get(_DRAMATIQ_MESSAGE_ENQUEUED_TIME_KEY)
        if message_enqueue_time:
            self.broker_queued_duration.record(
                now_millis - message_enqueue_time,
                {**current_message_attributes, **current_process_attributes},
            )
            self.broker_queued_duration.record(
                now_millis - message_enqueue_time,
                {**current_process_attributes},
            )
            del message.options[_DRAMATIQ_MESSAGE_ENQUEUED_TIME_KEY]

        # Remove from delayed metrics
        message_delayed_time = message.options.get(_DRAMATIQ_MESSAGE_DELAYED_TIME_KEY)
        if message_delayed_time:
            self.broker_delayed_duration.record(
                now_millis - message.options[_DRAMATIQ_MESSAGE_DELAYED_TIME_KEY],
                {**current_message_attributes, **current_process_attributes},
            )
            self.broker_delayed_duration.record(
                now_millis - message.options[_DRAMATIQ_MESSAGE_DELAYED_TIME_KEY],
                {**current_process_attributes},
            )
            del message.options[_DRAMATIQ_MESSAGE_DELAYED_TIME_KEY]

        # Attach trace context to the message processing, if any
        if "trace_ctx" not in message.options:
            return

        trace_ctx = extract(message.options["trace_ctx"])
        operation_name = utils.get_operation_name(
            "before_process_message", message.actor_name, retry_count
        )
        span_attributes = {_DRAMATIQ_MESSAGE_RETRY_COUNT_KEY: retry_count}

        span = self._tracer.start_span(
            operation_name,
            kind=trace.SpanKind.CONSUMER,
            context=trace_ctx,
            attributes=span_attributes,
        )

        activation = trace.use_span(span, end_on_exit=True)
        # This is horrible but read this: https://github.com/open-telemetry/opentelemetry-python/issues/2432
        # Basically, Tracer.start_span() only uses trace_ctx to set the parent span, but doesn't set the baggage.
        # So it is still up to the implementer, to manually retrieve said baggage, and attach it to the new span.
        ctx_with_baggage = context.get_current()
        for k, v in baggage.get_all(trace_ctx).items():
            ctx_with_baggage = baggage.set_baggage(k, v, context=trace_ctx)
        context.attach(ctx_with_baggage)

        activation.__enter__()  # pylint: disable=E1101
        utils.attach_span(self._span_registry, message.message_id, (span, activation))

    def after_process_message(
        self,
        _broker: dramatiq.Broker,
        message: dramatiq.Message,
        *,
        result=None,
        exception=None,
    ):
        # Calculate broker message metrics
        current_message_attributes = self._get_message_attributes(message)
        current_process_attributes = self._get_process_attributes()
        now_millis = _current_millis()
        message_process_time = message.options.get(_DRAMATIQ_MESSAGE_PROCESS_TIME_KEY)
        self.broker_processed_duration.record(
            now_millis - message_process_time,
            {**current_message_attributes, **current_process_attributes},
        )
        self.broker_processed_duration.record(
            now_millis - message_process_time,
            {**current_process_attributes},
        )
        del message.options[_DRAMATIQ_MESSAGE_PROCESS_TIME_KEY]
        if exception is not None:
            self.broker_failed_total.add(
                1, {**current_message_attributes, **current_process_attributes}
            )

        # Attach attributes to the exit span, and detach the span
        span, activation = utils.retrieve_span(self._span_registry, message.message_id)
        if span is None:
            return

        if span.is_recording():
            span_attributes = self._get_span_attributes("process", message)
            if exception is not None:
                span_attributes["dramatiq.exception"] = type(exception).__name__
                span_attributes["dramatiq.traceback"] = traceback.format_exc(limit=30)
                span_attributes["dramatiq.is_failed"] = True
            else:
                span_attributes["dramatiq.is_failed"] = False
            span.set_attributes(span_attributes)

        activation.__exit__(None, None, None)
        utils.detach_span(self._span_registry, message.message_id)

    def after_skip_message(
        self,
        _broker: dramatiq.Broker,
        message: dramatiq.Message,
        *,
        result=None,
        exception=None,
    ):
        # Calculate broker message metrics
        current_message_attributes = self._get_message_attributes(message)
        current_process_attributes = self._get_process_attributes()
        now_millis = _current_millis()
        message_process_time = message.options.get(_DRAMATIQ_MESSAGE_PROCESS_TIME_KEY)
        self.broker_processed_duration.record(
            now_millis - message_process_time,
            {**current_message_attributes, **current_process_attributes},
        )
        self.broker_processed_duration.record(
            now_millis - message_process_time,
            {**current_process_attributes},
        )
        del message.options[_DRAMATIQ_MESSAGE_PROCESS_TIME_KEY]
        if exception is not None:
            self.broker_failed_total.add(
                1, {**current_message_attributes, **current_process_attributes}
            )

    def before_enqueue(
        self, _broker: dramatiq.Broker, message: dramatiq.Message, delay
    ):
        current_message_attributes = self._get_message_attributes(message)
        current_process_attributes = self._get_process_attributes()
        self.broker_queued_total.add(
            1, {**current_message_attributes, **current_process_attributes}
        )
        self.broker_processed_total.add(
            0, {**current_message_attributes, **current_process_attributes}
        ) # this should be 0, as we are queueing the message, but will still trigger an update in the metric, so it will appear alongside the queued metric
        message.options[_DRAMATIQ_MESSAGE_ENQUEUED_TIME_KEY] = _current_millis()

        retry_count = message.options.get("retries", 0)
        operation_name = utils.get_operation_name(
            "before_enqueue", message.actor_name, retry_count
        )
        if "trace_ctx" in message.options:
            # If we enqueue a message that has a trace context, we should attach it to the span
            # This is needed for message retries
            trace_ctx = extract(message.options["trace_ctx"])
        else:
            trace_ctx = None
        span_attributes = {_DRAMATIQ_MESSAGE_RETRY_COUNT_KEY: retry_count}

        span = self._tracer.start_span(
            operation_name,
            kind=trace.SpanKind.PRODUCER,
            context=trace_ctx,
            attributes=span_attributes,
        )

        if span.is_recording():
            span.set_attributes(self._get_span_attributes("send", message))

        activation = trace.use_span(span, end_on_exit=True)
        activation.__enter__()  # pylint: disable=E1101

        utils.attach_span(
            self._span_registry,
            message.message_id,
            (span, activation),
            is_publish=True,
        )

        if "trace_ctx" not in message.options:
            message.options["trace_ctx"] = {}
        inject(message.options["trace_ctx"])

    def after_enqueue(
        self, _broker: dramatiq.Broker, message: dramatiq.Message, delay, exception=None
    ):
        _, activation = utils.retrieve_span(
            self._span_registry, message.message_id, is_publish=True
        )

        if activation is None:
            # no existing span found for message_id
            return

        activation.__exit__(None, None, None)
        utils.detach_span(self._span_registry, message.message_id, is_publish=True)


class DramatiqInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, broker: dramatiq.broker.Broker = None, **kwargs):
        tracer_provider = kwargs.get("tracer_provider")

        # pylint: disable=attribute-defined-outside-init
        self._tracer = trace.get_tracer(
            __name__,
            __version__,
            tracer_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )
        instrumentation_middleware = _InstrumentationMiddleware(self._tracer)

        if broker is None:
            broker = dramatiq.broker.get_broker()

        # first_middleware = broker.middleware[0] if broker.middleware else None
        # broker.add_middleware(instrumentation_middleware, before=type(first_middleware))
        broker.add_middleware(instrumentation_middleware)

    def _uninstrument(self, broker: dramatiq.broker.Broker = None, **kwargs):
        if broker is None:
            broker = dramatiq.broker.get_broker()
        broker.middleware = [
            m
            for m in broker.middleware
            if not isinstance(m, _InstrumentationMiddleware)
        ]
