import datetime

from time import process_time
import logging
import sys

import odinlogger
import jobconfig
import reference
import transformation

my_logger = odinlogger.setup_logging("main")


def create_reference(jobdata: {}) -> str:
    my_logger.info("Starting reference creation")
    reference.create_reference(jobdata=jobdata)
    my_logger.info("Ending reference creation")


def read_job_config() -> {}:
    my_logger.debug("Read the job config file for setup job")
    return jobconfig.read_job_config(sys.argv[1])


def get_ts():
    """
    get the current timestamp
    :return: current timestamp
    """
    return datetime.datetime.today()


def print_job_statistic(start_time_of_job, end_time_of_job, stats: str, end_status: str):
    my_logger.info("========================================================")
    my_logger.info("Job statistics : ")
    my_logger.info("Job started at : %s", start_time_of_job)
    my_logger.info("Job ended   at : %s", end_time_of_job)
    duration = end_time_of_job - start_time_of_job
    my_logger.info("Duration       : %s  ", duration)
    my_logger.info("CPU TIME       : %s  ", process_time())

    my_logger.info("Job ended with status %s", end_status)


if __name__ == '__main__':

    my_logger.info('Starting Odin\'s data migration tool version 1.0.0')
    my_logger.info('Starting with parameters  %s', str(sys.argv))
    start_time_of_job = get_ts()
    jobdata = read_job_config()
    result = "FAILED"
    my_logger.info("Performing job : %s",jobdata['job_name'])
    if jobdata['job_type'] == 'createReference':
        result=create_reference(jobdata=jobdata)
    elif jobdata['job_type'] == 'transformation':
        result=transformation.perform_transformation(jobdata=jobdata)
    else:
        my_logger.critical("Unknown job_type found. Please check : %s", jobdata['job_type'])
        sys.exit(99)
    end_time_of_job = get_ts()
    print_job_statistic(start_time_of_job, end_time_of_job, None, result)
    if result == "FAILED":
        sys.exit(99)
