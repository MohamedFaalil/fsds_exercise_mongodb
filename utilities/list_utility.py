class ListUtility:
    @staticmethod
    def is_index_exist_on_list(list, index):
        return True if index < len(list) else False

    @staticmethod
    def is_value_exist_on_list(list, value):
        return value in list
