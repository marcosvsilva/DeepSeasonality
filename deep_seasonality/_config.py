import json


class Config:

    def __init__(self, config_file):
        self.config_file = config_file

    def get_key(self, key):
        return self.__read_key_system_config(key)

    def __read_key_system_config(self, key_system):
        return self.__read_keys_config('system')[key_system]

    def __read_keys_config(self, key):
        return read_archive(self.config_file)[key]


class Token:

    def __init__(self, key_file):
        self.key_file = key_file

    def get_key(self, key):
        return self.__read_database(key)

    def __read_database(self, key):
        return self.__read_token_file('database')[key]

    def __read_token_file(self, key):
        return read_archive(self.key_file)[key]


def read_archive(file_name):
    try:
        with open(file_name, 'r') as file:
            archive_config = json.loads(file.read())

        return archive_config
    except Exception as fail:
        raise Exception('fail read config file {}, fail: {}'.format(file_name, fail))
