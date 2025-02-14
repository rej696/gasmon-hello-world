"""
The GasMon application
"""

import logging
import sys
import time

from gasmon.configuration import config
from gasmon.locations import get_locations
from gasmon.pipeline import FixedDurationSource, ComposedPipeline, RemoveDuplicates, AverageValuesPerMinute, ValuesPerLocation
from gasmon.receiver import QueueSubscription, Receiver
from gasmon.sink import Printer, SaveAveragePerMinToCSV

root_logger = logging.getLogger()
log_formatter = logging.Formatter('%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s')

file_handler = logging.FileHandler('GasMon.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(log_formatter)
root_logger.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)


def main():
    """
    Run the application.
    """

    # Get the list of valid sensor locations from S3
    s3_bucket = config['locations']['s3_bucket']
    locations_key = config['locations']['s3_key']
    locations = get_locations(s3_bucket, locations_key)
    for location in locations:
        print(f'{location}')

    # Create the pipeline steps that events will pass through when being processed
    run_time_seconds = int(config['run_time_seconds'])
    fixed_duration_source = FixedDurationSource(run_time_seconds)
    remove_duplicates = RemoveDuplicates()
    average_values = AverageValuesPerMinute()
    values_per_location = ValuesPerLocation(locations)
    pipeline = values_per_location.compose(average_values.compose(remove_duplicates.compose(fixed_duration_source)))

    # Create an SQS queue that will be subscribed to the SNS topic
    sns_topic_arn = config['receiver']['sns_topic_arn']
    with QueueSubscription(sns_topic_arn) as queue_subscription:

        # Process events as they come in from the queue
        receiver = Receiver(queue_subscription)
        # print all the data
        pipeline.sink(Printer(locations, values_per_location)).handle(receiver.get_events())
        values_per_location.plot_values.plot(values_per_location.event_location_container, False)
        SaveAveragePerMinToCSV(average_values)

        # Show final stats
        print(f'\nProcessed {fixed_duration_source.events_processed} events in {run_time_seconds} seconds')
        print(f'Events/s: {fixed_duration_source.events_processed / run_time_seconds:.2f}\n')
        print(f'Duplicated events skipped: {remove_duplicates.counter}')
        average_value_index = 0
        with open("average_values.csv", "w") as file:
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
