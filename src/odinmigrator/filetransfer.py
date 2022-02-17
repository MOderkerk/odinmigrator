import os.path
import time
import sys
from ftplib import FTP, FTP_TLS
import csv
from src.odinmigrator import odinlogger

my_logger = odinlogger.setup_logging("Filetransfer")


def __log_response(ftp: str):
    """
    log the response of the last ftp operation
    :param ftp: connection
    :return: None
    """
    my_logger.info(ftp)


def __read_secrets(server_input: str, secret_file: str) -> {}:
    """
       read the secret file , look for the server and return the user and password

       :param server_input: to look for
       :param secret_file: path to the password secrets
       :return: credentials or None
       """
    with (open(secret_file, "r")) as secret_file:
        reader = csv.DictReader(secret_file, delimiter=';')
        my_logger.info("server input: " + str(server_input))
        for row in reader:
            if row['server'] == server_input:
                return row
    return ''


def __connect_to_ftp(server_input: str, port: int, secret_file: str, tls: bool) -> FTP:
    """
    Create a connection to the ftp server
    :param server_input:  url of the server
    :param port:  port to use
    :param secret_file: path to the secret file
    :return: ftp connection
    """

    credentials = __read_secrets(server_input=server_input, secret_file=secret_file)
    if credentials is not None:
        if not tls:
            ftp_obj = FTP()
            ftp_con = ftp_obj.connect(host=server_input, port=port, timeout=3600)
            __log_response(ftp_con)
        else:
            ftp_obj = FTP_TLS()
            ftp_con = ftp_obj.connect(host=server_input, port=port)
            __log_response(ftp_con)
        ftp_con = ftp_obj.login(user=credentials['user'], passwd=credentials['pw'])
        __log_response(ftp_con)
        ftp_obj.set_pasv(True)
        __log_response(ftp_obj.pwd())
        return ftp_obj
    else:
        my_logger.critical(
            "No credentials found in secretfile. Please provide the right secret file or add credentials  ")
        return None


def __show_file_data(filename: str):
    """
    Display file data of local file
    :param filename:  to look for
    :return: None
    """
    os_stats = os.stat(filename);

    my_logger.info("File info for " + filename + "-> Stats: Size:" + str(os_stats.st_size) + "  -  Created  "
                   + time.ctime(os_stats.st_mtime))


def __list_dir(serv: str, port: int, tls: bool, secret_file: str,
               remote_folder: str, target_folder: str,
               file: str):
    """
    List the content of a remote dir and save it in a local file
    :param tls: use tls or not
    :param port: port to use
    :param serv: server
    :param secret_file: file with the credentials
    :param remote_folder:  remote folder
    :param target_folder: local folder
    :param file: name of the file
    :return: status
    """
    my_logger.info("Starting download")

    result = "FAILED"
    ftp = __connect_to_ftp(server_input=serv, port=port, tls=tls, secret_file=secret_file)

    if ftp is not None:
        ftp_con = ftp.cwd(remote_folder)
        __log_response(ftp_con)
        ftp_con = ftp.pwd()
        __log_response(ftp_con)
        filename = target_folder + "/" + file
        with(open(filename, "w")) as lf:

            ftp_con = ftp.retrlines('LIST ', lf.write)
            __log_response(ftp_con)

        ftp_con = ftp.quit()
        __log_response(ftp_con)
        result = "SUCCESS"
    else:
        my_logger.critical(
            "Aborting ftp due to missing connection")

        sys.exit(99)

    return result


def __download_file(serv: str, port: int, tls: bool, secret_file: str,
                    remote_folder: str,
                    target_folder: str,
                    file: str) -> str:
    """
    Download a file
    :param tls: use tls or not
    :param port: port to use
    :param serv: server
    :param secret_file: file with the credentials
    :param remote_folder:  remote folder
    :param target_folder: local folder
    :param file: name of the file
    :return: status
    """

    my_logger.info("Starting download")

    result = "FAILED"
    ftp = __connect_to_ftp(server_input=serv, port=port, tls=tls, secret_file=secret_file)

    if ftp is not None:
        ftp_con = ftp.cwd(remote_folder)
        __log_response(ftp_con)
        ftp_con = ftp.pwd()
        __log_response(ftp_con)
        filename = target_folder + "/" + file
        with(open(filename, "wb")) as lf:
            ftp_con = ftp.retrbinary("RETR " + file, lf.write)
            __log_response(ftp_con)
            __show_file_data(filename)
        ftp_con = ftp.quit()
        __log_response(ftp_con)
        result = "SUCCESS"
    else:
        my_logger.critical(
            "Aborting ftp due to missing connection")

        sys.exit(99)

    return result


def __upload_file(serv: str, port: int, tls: bool, secret_file: str,
                  remote_folder: str,
                  source_folder: str,
                  file: str) -> str:
    """
    Download a file
    :param tls: use tls or not
    :param port: port to use
    :param serv: server
    :param secret_file: file with the credentials
    :param remote_folder:  remote folder
    :param source_folder: local folder
    :param file: name of the file
    :return: status
    """

    my_logger.info("Starting upload")

    result = "FAILED"
    ftp = __connect_to_ftp(server_input=serv, port=port, tls=tls, secret_file=secret_file)

    if ftp is not None:
        ftp_con = ftp.cwd(remote_folder)
        __log_response(ftp_con)
        ftp_con = ftp.pwd()
        __log_response(ftp_con)
        filename = source_folder + "/" + file
        with(open(filename, "rb")) as lf:
            ftp_con = ftp.storbinary("STOR " + file, lf)
            __log_response(ftp_con)
            ftp.retrlines('LIST ' + file)
        ftp_con = ftp.quit()
        __log_response(ftp_con)
        result = "SUCCESS"
    else:
        my_logger.critical(
            "Aborting ftp due to missing connection")

        sys.exit(99)

    return result


def transfer(jobdata: {}):
    my_logger.info("Starting: " + jobdata['job_name'])
    host = jobdata['server'];
    tls = jobdata['tls']
    port = jobdata['port']
    ftp_secret = jobdata['ftp-secret-file']

    for job in jobdata['files']:
        if job['file']['command'] == 'get':
            __download_file(serv=host, port=port, tls=tls, secret_file=ftp_secret,
                            remote_folder=job['file']['remotefolder'],
                            target_folder=job['file']['target'], file=job['file']['filename'])
        elif job['file']['command'] == 'list':
            __list_dir(serv=host, port=port, tls=tls, secret_file=ftp_secret,
                       remote_folder=job['file']['remotefolder'],
                       target_folder=job['file']['target'], file=job['file']['filename'])
        elif job['file']['command'] == 'put':
            __upload_file(serv=host, port=port, tls=tls, secret_file=ftp_secret,
                          remote_folder=job['file']['remotefolder'],
                          source_folder=job['file']['source'], file=job['file']['filename'])
        else:
            my_logger.critical(
                "Aborting ftp due to unknown command")
            sys.exit(99)
