"""
A module consisting of sinks that the processed events will end up at.
"""

from abc import abstractmethod, ABC
import time
from gasmon.pipeline import EventLocationContainer, PlotValues
import matplotlib.pyplot as plt


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
    def __init__(self, locations, values_per_location):
        self.locations = locations
        self.values_per_location = values_per_location

    def handle(self, events):
        event_location_container = EventLocationContainer(self.locations)
        for event in events:
            event_location_container.add_event(event)
            self.values_per_location.plot_values.plot(event_location_container, False)
            print(event)
        plot_values = PlotValues()
        plot_values.plot(event_location_container, True)
        SaveAverageAtLocationToCSV(event_location_container)
        for location in event_location_container.values_at_locations:
            print(f"x:{location.x_location}, y:{location.y_location}, average value:{location.average()}")


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


class SaveAverageAtLocationToCSV:
    def __init__(self, event_location_container):
        with open("average_values_per_location.csv", "w+") as file:
            file.write(f"{time.ctime()}\nx,y,average value\n")
            for location in event_location_container.values_at_locations:
                file.write(f"{location.x_location},{location.y_location},{location.average()}\n")
