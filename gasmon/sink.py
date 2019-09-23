"""
A module consisting of sinks that the processed events will end up at.
"""

from abc import abstractmethod, ABC

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
