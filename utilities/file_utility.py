from utilities.logger_utility import LogUtility
import os


class FileUtility:
    def __init__(self, file_location):
        self._fileReader = self.__get_readable_file_pointer(file_location)

    def __get_readable_file_pointer(self, file_location):
        if not os.path.exists(file_location) and not os.path.isfile(file_location):
            LogUtility.write_warning('file-processing.log', "File '{}' does not exist to read.".format(file_location))
            raise Exception("'{}' is not exists".format(file_location))
        try:
            return open(file_location, 'r')
        except Exception as ex:
            LogUtility.write_error('file-processing.log',
                                  "Exception occurred, while reading file `{}` (->on FileHandler=>setFilePointer()<-) : {}".format(file_location, str(ex)))
            raise Exception("could not able to read file `{}`".format(file_location))
