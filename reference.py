from sqlite3 import Connection, Cursor

import odinlogger
import csv
import sqlite3

my_logger = odinlogger.setup_logging("Reference")


def create_database(reference_name: str, output_file: str, field_type: str) -> Connection:
    """
    Create a new database
    :param field_type: type of the ref column
    :param reference_name: name of the reference
    :param output_file: target file of the database
    :return: sqlite3 connection
    """
    con = sqlite3.connect(output_file)
    my_logger.info("Creating new reference table : %s", reference_name)
    create_stmt = "CREATE TABLE IF NOT EXISTS " + reference_name \
                  + " (OLD_VALUE " + field_type + " PRIMARY KEY asc , NEW_VALUE " + field_type + ")"

    con.execute(create_stmt)
    count = con.execute("SELECT count(*) from " + reference_name)
    if count.fetchall()[0][0] > 0:
        my_logger.info("Found old entries in database. Cleaning up")
        con.execute("DELETE FROM " + reference_name)
    return con


def create_ref_rule_1_database(reference_name: str, input_data: str, field_type: str, start_value: int, increment: str,
                               negate: bool,
                               output_file: str) -> str:
    """
    Create a reference in a sqlite database
    :param field_type: type of ref field
    :param reference_name:  name of the reference . this will be the table name in the database
    :param input_data:  csv file with all old keys
    :param start_value:  start value of the new key
    :param increment:  steps for increment the new value
    :param negate:  flag indicating if the new value will be multiplied with -1
    :param output_file: filename of the database
    :return: status
    """
    con = create_database(reference_name=reference_name, output_file=output_file, field_type=field_type)

    with open(input_data, newline='') as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel', delimiter=';')
        i: int = 0
        o: int = 0
        current_id = start_value
        my_logger.info("Creating new references with start id :%s", start_value)
        for row in reader:
            i = i + 1
            output_value = current_id
            if negate:
                output_value = output_value * -1

            output = [row['input'], output_value]
            con.execute("INSERT INTO " + reference_name + " VALUES (?,?)", output)

            o = o + 1
            current_id = current_id + int(increment)

            if i % 100 == 0:
                my_logger.info("%s references created. Last key pair generated: %s", str(i), str(output))
                con.commit()

        con.commit()
        count_after_end = con.execute("SELECT count(*) from " + reference_name)

        my_logger.info("Creation of reference ended.")
        my_logger.info("Count input records : %s ; Count output records : %s", i, o)
        my_logger.info("Count records in database : %s", count_after_end.fetchall()[0][0])
        con.close()
    return "SUCCESS"


def create_ref_rule_2_database(reference_name: str, field_type: str, input_data: str, output_file: str) -> str:
    con = create_database(reference_name=reference_name, output_file=output_file, field_type=field_type)

    with open(input_data, newline='') as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel', delimiter=';')
        my_logger.info("Creating new reference with imported data")
        i: int = 0
        o: int = 0
        for row in reader:
            i = i + 1

            output = [row['input'], row['output']]
            con.execute("INSERT INTO " + reference_name + " VALUES (?,?)", output)

            o = o + 1

            if i % 100 == 0:
                my_logger.info("%s references created. Last key pair generated: %s", str(i), str(output))
                con.commit()

        con.commit()
        count_after_end = con.execute("SELECT count(*) from " + reference_name)

        my_logger.info("Creation of reference ended.")
        my_logger.info("Count input records : %s ; Count output records : %s", i, o)
        my_logger.info("Count records in database : %s", count_after_end.fetchall()[0][0])
        con.close()
    return "SUCCESS"


def create_ref_rule_1_csv(reference_name: str, input_data: str, start_value: int, increment: str, negate: bool,
                          output_file: str) -> str:
    """
    Create a new reference with rule 1
    :param negate: flag indicating if the new key is negated or not
    :param reference_name:  name of the reference
    :param input_data:  original keys
    :param start_value: integer for generating new id
    :param increment :  how to increment
    :param output_file: name of the target file
    :return: status information
    """
    my_logger.info(input_data)
    with open(output_file, 'w+', newline='', encoding='UTF8') as target_file:
        with open(input_data, newline='') as csvfile:
            reader = csv.DictReader(csvfile, dialect='excel', delimiter=';')
            writer = csv.writer(target_file, delimiter=';',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)

            writer.writerow(['old_value', 'new_value'])

            i: int = 0
            o: int = 0

            current_id = start_value
            my_logger.info("Creating new references with start id for %s :%s", start_value, reference_name)
            for row in reader:
                i = i + 1
                output_value = current_id
                if negate:
                    output_value = output_value * -1
                output = [row['input'], output_value]
                writer.writerow(output)
                o = o + 1
                current_id = current_id + int(increment)

                if i % 10 == 0:
                    my_logger.info("%s references created. Last input data processed: %", str(i), str(output))

            my_logger.info("Creation of reference ended.")
            my_logger.info("Count input records : %s ; Count output records : %s", i, o)
    return "SUCCESS"


def open_reference_database(reference_file: str) -> Cursor:
    """
    Create a connection to a reference database
    :param reference_file: path to the databse file
    :return:  SQLITE connection
    """
    my_logger.info("Open connection to %s", reference_file)
    return sqlite3.connect(reference_file).cursor()


def close_database_connection(cursor: Cursor):
    """
    Close the database connection
    :param cursor: of the database connection
    :return: none
    """
    cursor.connection.close()


def read_reference_entry(cursor: Cursor, reference_name: str, old_value: int):
    """
    Read a reference entry from the database
    :param cursor: sqlite cursor to use
    :param reference_name: name of the reference
    :param old_value: value to get the corresponding new value
    :return: corresponding new value
    """
    select_stmt = "SELECT NEW_VALUE FROM " + reference_name + " where OLD_VALUE = ? "
    parameter = [old_value]
    result = cursor.execute(select_stmt, parameter).fetchone()
    if result is not None:
        my_logger.debug("Found %s after looking for %s", result[0], old_value)
        return result[0]
    else:
        my_logger.error("Found no new key after looking for %s", old_value)
        return None


def create_reference(jobdata: {}) -> str:
    """
    Create a new reference from the given data and rule
    :param jobdata:
    :return:
    """
    parameter = jobdata['parameter']
    my_logger.info("Creating '%s' with rule '%s' from file '%s'", parameter['reference_name'],
                   parameter['creation_rule'], parameter['input_data'])
    result = "FAILED"
    if parameter['creation_rule'] == 1:
        if parameter['output_type'] == 'csv':
            result = create_ref_rule_1_csv(reference_name=parameter['reference_name'],
                                           input_data=parameter['input_data'],
                                           start_value=parameter['start_value'],
                                           increment=parameter['increment'],
                                           negate=parameter['negate'],
                                           output_file=parameter['target_file'])
        elif parameter['output_type'] == 'db':
            result = create_ref_rule_1_database(reference_name=parameter['reference_name'],
                                                input_data=parameter['input_data'],
                                                start_value=parameter['start_value'], increment=parameter['increment'],
                                                negate=parameter['negate'],
                                                output_file=parameter['target_file'],
                                                field_type=parameter['field_type'])
    if parameter['creation_rule'] == 2:
        result = create_ref_rule_2_database(reference_name=parameter['reference_name'],
                                            input_data=parameter['input_data'],
                                            output_file=parameter['target_file'], field_type=parameter['field_type'])
    return result
