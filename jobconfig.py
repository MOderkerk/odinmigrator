import yaml

import odinlogger

my_logger = odinlogger.setup_logging("Jobconfig reader")


def read_job_config(filename: str) -> {}:
    """
    Read the job config file
    :param filename: path to the job controll file
    :return:  jobdata
    """
    with open(filename, "r") as stream:
        my_logger.debug('Reading: ' +filename)
        try:
            jobdata = yaml.safe_load(stream)
            my_logger.debug(jobdata)
            my_logger.info("Jobconfig file read ")
        except yaml.YAMLError as exc:
            my_logger.critical(exc)

        return jobdata