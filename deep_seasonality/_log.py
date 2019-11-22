from datetime import datetime


def generate_log(file, message):
    with open(file, 'a') as file:
        file.writelines('{}: {}!\n'.format(datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                                           message.replace('\n', ' -- ').lower()))
