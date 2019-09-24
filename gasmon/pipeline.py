"""
A module consisting of pipeline steps that processed events will pass through.
"""

from abc import ABC, abstractmethod
from collections import deque, namedtuple
import logging
from time import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Pipeline(ABC):
    """
    An abstract base class for pipeline steps.
    """

    @abstractmethod
    def handle(self, events):
        """
        Transform the given stream of events into a processed stream of events.
        """
        pass

    def sink(self, sink):
        """
        Funnel events from this Pipeline into the given sink.
        """
        return PipelineWithSink(self, sink)

    def compose(self, other):
        return ComposedPipeline(self, other)

class ComposedPipeline(Pipeline):
    def __init__(self, first, second):
        self.first = first
        self.second = second

    def handle(self, events):
        return self.second.handle(self.first.handle(events))


class PipelineWithSink(Pipeline):
    """
    A Pipeline with a final processing step (a Sink).
    """

    def __init__(self, pipeline, sink):
        """
        Create a Pipeline with a Sink.
        """
        self.pipeline = pipeline
        self.sink = sink

    def handle(self, events):
        """
        Handle events by first letting the pipeline process them, then 
        passing the result to the sink
        """
        self.sink.handle(self.pipeline.handle(events))


class FixedDurationSource(Pipeline):
    """
    A Pipeline step that processes events for a fixed duration.
    """

    def __init__(self, run_time_seconds):
        """
        Create a FixedDurationSource which will run for the given duration.
        """
        self.run_time_seconds = run_time_seconds
        self.events_processed = 0

    def handle(self, events):
        """
        Pass on all events from the source, but cut it off when the time limit is reached.
        """

        # Calculate the time at which we should stop processing
        end_time = time() + self.run_time_seconds
        logger.info(f'Processing events for {self.run_time_seconds} seconds')

        # Process events for as long as we still have time remaining
        for event in events:
            if time() < end_time:
                logger.debug(f'Procesing event: {event}')
                self.events_processed += 1
                yield event
            else:
                logger.info('Finished processing events')
                return


class RemoveDuplicates(Pipeline):
    def __init__(self):
        self.counter = 0
        self.time = 300  # time in seconds
        self.event_id_cache = set()
        self.start_time = time()

    def handle(self, events):
        for event in events:
            current_time = time()
            if current_time - self.start_time >= self.time:
                self.event_id_cache = set()
                self.start_time = time()
            if event.event_id in self.event_id_cache:
                self.counter += 1
            else:
                self.event_id_cache.add(event.event_id)
                yield event


class AverageValues(Pipeline):
    def __init__(self):
        self.counter = 0
        self.start_time = time()  # time in seconds
        self.total_value = 0
        self.average_values = []

    def handle(self, events):
        for event in events:
            self.total_value += event.value
            self.counter += 1
            current_time = time()
            if current_time - self.start_time >= 60:
                self.average_values.append(self.total_value / self.counter)
                self.total_value = 0
                self.counter = 0
                self.start_time = time()
            yield event

