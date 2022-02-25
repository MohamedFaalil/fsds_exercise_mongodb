from utilities.logger_utility import LogUtility
from utilities.list_utility import ListUtility


class CsvFileService:
    @staticmethod
    def get_dictionary_data_from_csv(csv_file_util):
        try:
            csv_column_list = csv_file_util.get_csv_column_name_as_list()
            csv_data_list = csv_file_util.get_csv_values_as_list()
            lst = []
            for row in csv_data_list:
                idea_dictionary = CsvFileService.__get_ideal_dictionary(csv_column_list, csv_data_list)
                ideal_dictionary_key_list = list(idea_dictionary.keys())
                for index in range(len(ideal_dictionary_key_list)):
                    if ListUtility.is_index_exist_on_list(row, index):
                        idea_dictionary[ideal_dictionary_key_list[index]] = row[index]
                    else:
                        # removing empty column on dictionary
                        idea_dictionary.pop(ideal_dictionary_key_list[index])

                lst.append(idea_dictionary)
            return lst
        except Exception as ex:
            LogUtility.write_error('csv-manipulation.log',
                                   'Exception occurred (->on CsvFileService=>getDictionaryDataFromCsvList()<-) :'
                                   + str(ex))
            raise Exception("CSV binding with dictionary process is broken.")

    @staticmethod
    def __get_length_of_largest_row_of_data(csv_data_list):
        length = len(csv_data_list[0])
        for index in range(1, len(csv_data_list)):
            each_row_length = len(csv_data_list[index])
            if each_row_length > length:
                length = each_row_length
        return length

    @staticmethod
    def __get_ideal_dictionary(csv_column_list, csv_data_list):
        largest_row_length = CsvFileService.__get_length_of_largest_row_of_data(csv_data_list)
        dictionary = {}
        for i in range(largest_row_length):
            if ListUtility.is_index_exist_on_list(csv_column_list, i):
                dictionary[csv_column_list[i]] = None
            else:
                column_name = "COLUMN - {}".format(i - (len(csv_column_list) - 1))
                dictionary[column_name] = None
        return dictionary
