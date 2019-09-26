"""
A module consisting of pipeline steps that processed events will pass through.
"""

from abc import ABC, abstractmethod
from collections import deque, namedtuple
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm
import numpy as np
import logging
import random
import time

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
        end_time = time.time() + self.run_time_seconds
        logger.info(f'Processing events for {self.run_time_seconds} seconds')

        # Process events for as long as we still have time remaining
        for event in events:
            if time.time() < end_time:
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
        self.start_time = time.time()

    def handle(self, events):
        for event in events:
            current_time = time.time()
            if current_time - self.start_time >= self.time:
                self.event_id_cache = set()
                self.start_time = time.time()
            if event.event_id in self.event_id_cache:
                self.counter += 1
            else:
                self.event_id_cache.add(event.event_id)
                yield event


class AverageValuesPerMinute(Pipeline):
    def __init__(self):
        self.counter = 0
        self.start_time = time.time()
        self.average_values_timestamp = []
        self.total_value = 0
        self.average_values = []

    def handle(self, events):
        for event in events:
            self.total_value += event.value
            self.counter += 1
            current_time = time.time()
            if current_time - self.start_time >= 60:
                self.average_values.append(self.total_value / self.counter)
                self.average_values_timestamp.append(current_time)
                self.total_value = 0
                self.counter = 0
                self.start_time = time.time()
            yield event


class MatchLocation(Pipeline):
    def __init__(self, locations):
        self.locations = locations

    def handle(self, events):
        for old_event in events:
            for location in self.locations:
                if old_event.location_id == location.id:
                    event = namedtuple("event", "x_location y_location event_id value timestamp")
                    matched_event = event(
                        location.x, location.y, old_event.event_id, old_event.value, old_event.timestamp
                    )
                    yield matched_event


class ValuesPerLocation(Pipeline):
    def __init__(self, locations):
        self.locations = locations
        self.plot_values = PlotValues()
        self.event_location_container = EventLocationContainer(self.locations)
        self.start_time = 0
        self.current_time = 0

    def handle(self, events):
        for event in events:
            self.event_location_container.add_event(event)
            self.current_time = time.time()
            # if self.current_time - self.start_time >= 4:
            # plt.close(self.plot_values.fig)
            # self.plot_values.plot(self.event_location_container, False)
            self.start_time = time.time()
            yield event


class EventLocationContainer:
    def __init__(self, locations):
        self.values_at_locations = []
        for location in locations:
            self.values_at_locations.append(ValuesAtLocation(location))

    def add_event(self, event):
        for value_at_location in self.values_at_locations:
            if event.location_id == value_at_location.id:
                value_at_location.add_value(event.value)

    def get_reported_locations(self):
        return filter(
            lambda l: len(l.values) > 0,
            self.values_at_locations
        )


class ValuesAtLocation:
    def __init__(self, location):
        self.x_location = location.x
        self.y_location = location.y
        self.id = location.id
        self.values = []
        self.counter = 0

    def add_value(self, value):
        self.values.append(value)

    def average(self):
        total = 0
        for value in self.values:
            total += value
            self.counter += 1
        if self.counter == 0:
            return total
        return total / len(self.values)


class PlotValues:
    def __init__(self):
        plt.ion()
        self.fig = plt.figure()
        # self.ax = self.fig.add_subplot(111, projection="3d")
        self.ax = Axes3D(self.fig)
        self.x = []
        self.y = []
        self.z = []
        self.ax.set_xlabel("X Direction")
        self.ax.set_ylabel("Y Direction")
        self.ax.set_zlabel("Average Value")
        self.colour_index = 0
        # colour_switch = [
        #     "viridis", "plasma", "inferno", "magma", "cividis"
        # ]
        self.colour_switch = [
            "Purples", "BuPu", "Blues", "GnBu", "Greens", "YlGn", "YlOrBr", "Oranges", "OrRd", "Reds", "PuRd"
        ]

    def plot(self, event_location_container, finished):
        self.x = []
        self.y = []
        self.z = []
        # self.fig = plt.figure()
        # self.ax = Axes3D(self.fig)
        # self.ax = self.fig.add_subplot(111, projection="3d")
        for location in event_location_container.get_reported_locations():
            self.x.append(location.x_location)
            self.y.append(location.y_location)
            self.z.append(location.average())
            # self.z.append(location.values[-1])
        if len(self.x) < 3 or len(self.y) < 3:
            return
        self.ax.cla()
        if self.colour_index == len(self.colour_switch):
            self.colour_index = 0
        self.ax.plot_trisurf(self.x, self.y, self.z, cmap=self.colour_switch[self.colour_index])
        self.colour_index += 1
        # self.ax.plot_trisurf(self.x, self.y, self.z, cmap=random.choice(colour_switch))
        # print("plot opened")
        if not finished:
            # plt.show(block=False)
            # plt.pause(2)
            self.fig.canvas.draw()
            self.fig.canvas.flush_events()
            return
        plt.show(block=False)
