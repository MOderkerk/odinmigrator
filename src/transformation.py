import csv
import sys
from sqlite3 import Cursor

from src import reference

from src import odinlogger

my_logger = odinlogger.setup_logging("Transformation")


def read_reference(cursor: Cursor, reference_name: str, old_value):
    """
    read a reference entry
    :param cursor: for db access
    :param reference_name: name of the reference
    :param old_value: old value to look for
    :return: found key or none
    """
    my_logger.debug("Reading reference %s", reference_name)
    return reference.read_reference_entry(cursor=cursor, reference_name=reference_name, old_value=old_value);


def show_transformation_result(input_counter, output_counter, error_counter):
    """
    Show the result of the transformation
    :param input_counter:  line read
    :param output_counter:  line written to output
    :param error_counter:  transformations ended in error
    :return: none
    """
    my_logger.info("Transformation ended with :")
    my_logger.info("Rows read %i - transformation successful : %i - transformation in error: %i", input_counter,
                   output_counter, error_counter)


def perform_transformation(jobdata: any) -> str:
    """
    Performing a transformation of a file
    :param jobdata: configuration of the transformation
    :return:  status
    """
    my_logger.info("Starting transformation with parameters %s", str(jobdata))
    result = "SUCCESS"
    transformation_rules = jobdata['parameter']['transformation_rules']
    error_filename = str(jobdata['job_name']).replace(' ', '_') + "_error.txt";
    with (open(jobdata['parameter']['input_data'], "r", encoding='UTF8')) as source_data:
        with (open(jobdata['parameter']['target_file'], "w", encoding='UTF8')) as target_data:
            with (open(error_filename, "w", encoding='UTF8')) as error_file:
                reader = csv.reader(source_data, dialect='excel', delimiter=';')
                writer = csv.writer(target_data, delimiter=';',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
                error_writer = csv.writer(error_file, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)

                my_logger.info("Number of rules of the job: %i ", len(transformation_rules))
                if jobdata['parameter']['reference_database'] is not None:
                    cursor = reference.open_reference_database(jobdata['parameter']['reference_database'])
                error_counter = 0
                input_counter = 0
                output_counter = 0
                for row in reader:
                    input_counter = input_counter + 1
                    has_error = False
                    converted_data = [''] * len(transformation_rules)
                    error_data = []
                    for rule in transformation_rules:
                        if rule['rule'] == 'copy':
                            converted_data[rule['fieldpos']] = row[rule['sourcefield']]
                        elif rule['rule'] == 'ref':
                            new_value = read_reference(cursor=cursor, reference_name=rule['reference_name'],
                                                       old_value=row[rule['sourcefield']])
                            if new_value is None:
                                for field in row:
                                    error_data.append(field)
                                error_data.append("No reference found in database")
                                has_error = True
                            else:
                                converted_data[rule['fieldpos']] = new_value

                        elif rule['rule'] == 'const':
                            converted_data[rule['fieldpos']] = str(rule['value'])
                        else:
                            my_logger.critical("Unknown conversion rule '%s' found in job config for fieldpos %s ",
                                               rule['rule'], rule['fieldpos'])
                            sys.exit(99)
                    if has_error:
                        error_writer.writerow(error_data)
                        error_counter = error_counter + 1
                    else:
                        writer.writerow(converted_data)
                        output_counter = output_counter + 1
                show_transformation_result(input_counter, output_counter, error_counter)

    return result
