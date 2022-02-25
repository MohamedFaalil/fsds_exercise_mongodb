import csv
from utilities.file_utility import FileUtility
from utilities.logger_utility import LogUtility


class CsvFileUtility(FileUtility):
    def __init__(self, csv_file_location, is_column_less=False):
        """If CSV data does not contain column(names) SET is_column_less=True; Default value is False"""
        super(CsvFileUtility, self).__init__(csv_file_location)
        self.__csvFileAsList = list(self.__get_csv_file_pointer(csv_file_location))
        self.is_column_less = is_column_less

    def get_csv_column_name_as_list(self):
        return [] if self.is_column_less else self.__csvFileAsList[0]

    def get_csv_values_as_list(self):
        return self.__csvFileAsList[0::] if self.is_column_less else self.__csvFileAsList[1::]

    ############################################################
    # ------------ Private Methods--------------################
    ############################################################

    def __get_csv_file_pointer(self, file_name):
        try:
            return csv.reader(self._fileReader, delimiter=';')
        except Exception as ex:
            LogUtility.write_error('csv-file-processing.log',
                                  "Exception occurred while reading csv file `{}` (->on CsvFileUtility=>__getCsvFilePointer()<-) : {}".format(file_name, str(ex)))
            raise Exception('could not able to read CSV file.')
