from datetime import date
from _processing import Processing
from _log import generate_log


def generate_directories():
    general_path = "C:\\Jave\\Seasonality"
    sqls_path = "{}\\sqls".format(general_path)
    dataset_path = "{}\\dataset".format(general_path)

    key_file = "{}\\key\\key.token".format(general_path)
    config_file = "{}\\config\\config.cfg".format(general_path)

    today = date.today().strftime("%d%b%Y")
    log_file = "{}\\logs\\log\\log_{}.log".format(general_path, today)
    log_fail_file = "{}\\logs\\log_fail\\log_fail_{}.log".format(general_path, today)

    return {'sqls_path': sqls_path, 'dataset_path': dataset_path, 'key_file': key_file, 'config_file': config_file,
            'log_file': log_file, 'log_fail_file': log_fail_file}


def main():
    try:
        process = Processing(generate_directories())
        process.process()
    except Exception as fail:
        generate_log(generate_directories['log_fail_file'], 'Except Interrupt System! fail: {}'.format(fail))


if __name__ == '__main__':
    main()
