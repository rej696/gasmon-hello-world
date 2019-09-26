"""
A module consisting of sinks that the processed events will end up at.
"""

from abc import abstractmethod, ABC
import time
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np


class Sink(ABC):
    """
    An abstract base class for pipeline sinks.
    """

    @abstractmethod
    def handle(self, events):
        """
        Handle each of the given stream of events.
        """
        pass


class Printer(Sink):

    def __init__(self):
        pass

    def handle(self, events):
        for event in events:
            print(event)


class SaveAveragePerMinToCSV:
    def __init__(self, average_values):
        average_value_index = 0
        with open("average_values_per_minute.csv", "w") as file:
            file.write("date,hour,minute,hours decimal,average value\n")
            for average_value in average_values.average_values:
                local_time_struct = time.localtime(average_values.average_values_timestamp[average_value_index])
                local_time_string_csv = time.strftime("%d/%m/%Y,%H,%M", local_time_struct)
                local_time_string_hour = time.strftime("%H", local_time_struct)
                local_time_string_min = time.strftime("%M", local_time_struct)
                local_time_string_hour_decimal = int(local_time_string_min) / 60
                hours_decimal = f"{int(local_time_string_hour) + local_time_string_hour_decimal}"
                local_time_string_print = time.strftime("%d/%m/%Y %H:%M:%S", local_time_struct)
                file.write(f"{local_time_string_csv},{hours_decimal},{average_value}\n")
                print(f'Average Value at {local_time_string_print}: {average_value}')
                average_value_index += 1


class EventLocationContainer:
    def __init__(self, locations):
        self.values_at_locations = []
        for location in locations:
            self.values_at_locations.append(ValuesAtLocation(location))

    def add_event(self, event):
        for value_at_location in self.values_at_locations:
            if event.location_id == value_at_location.id:
                value_at_location.add_value(event.value)


class ValuesAtLocation:
    def __init__(self, location):
        self.x_location = location.x
        self.y_location = location.y
        self.id = location.id
        self.values = []

    def add_value(self, value):
        self.values.append(value)

    def average(self):
        total = 0
        for value in self.values:
            total += value
        return total / len(self.values)


class ValuesPerLocation(Sink):
    def __init__(self, locations):
        self.locations = locations

    def handle(self, events):
        event_location_container = EventLocationContainer(self.locations)
        for event in events:
            event_location_container.add_event(event)
            print(event)
        PlotValues(event_location_container)
        SaveAverageAtLocationToCSV(event_location_container)
        for location in event_location_container.values_at_locations:
            print(f"x:{location.x_location}, y:{location.y_location}, average value:{location.average()}")


class PlotValues:
    def __init__(self, event_location_container):
        fig = plt.figure()
        ax = fig.add_subplot(111, projection="3d")
        x = []
        y = []
        z = []
        for location in event_location_container.values_at_locations:
            x.append(location.x_location)
            y.append(location.y_location)
            z.append(location.average())
        ax.plot_trisurf(x, y, z, cmap="viridis")
        ax.set_xlabel("X Direction")
        ax.set_ylabel("Y Direction")
        ax.set_zlabel("Average Value")
        plt.show()

class SaveAverageAtLocationToCSV:
    def __init__(self, event_location_container):
        with open("average_values_per_location.csv", "w+") as file:
            file.write(f"{time.ctime()}\nx,y,average value\n")
            for location in event_location_container.values_at_locations:
                file.write(f"{location.x_location},{location.y_location},{location.average()}\n")
