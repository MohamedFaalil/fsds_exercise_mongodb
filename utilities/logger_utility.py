from datetime import date
import logging as log


class LogUtility:
    @staticmethod
    def write_error(logging_file_name, msg):
        log = LogUtility.__get_logger_object(logging_file_name)
        log.error(msg)
        log.shutdown()

    @staticmethod
    def write_warning(logging_file_name, msg):
        log = LogUtility.__get_logger_object(logging_file_name)
        log.warning(msg)
        log.shutdown()

    @staticmethod
    def write_info(logging_file_name, msg):
        log = LogUtility.__get_logger_object(logging_file_name)
        log.info(msg)
        log.shutdown()

    ############################################################
    # ------------ Private Methods--------------################
    ############################################################

    @staticmethod
    def __get_logger_object(logging_file_name):
        log_file = "./logs/{file_name}".format(
            file_name=LogUtility.__get_logging_file_name_by_prepending_current_date(logging_file_name))
        log.basicConfig(filename=log_file, level=log.DEBUG, format="%(levelname)s %(asctime)s %(message)s")
        return log

    @staticmethod
    def __get_logging_file_name_by_prepending_current_date(logging_file_name):
        """Every day there will be new logging file"""
        return "{date}-{file_name}".format(
            date=date.today().strftime("%d-%m-%Y"),
            file_name=logging_file_name
        )
