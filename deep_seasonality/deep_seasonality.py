from datetime import date
from _processing import Processing
from _log import generate_log


def generate_directories():
    general_path = r"C:\Jave\Seasonality"
    sqls_path = r"{}\sqls".format(general_path)
    dataset_path = r"{}\dataset".format(general_path)
    key_file = r"{}\key\key.token".format(general_path)

    today = date.today().strftime("%d%b%Y")
    log_file = r"{}\logs\log\log_{}.log".format(general_path, today)
    log_fail_file = r"{}\logs\log_fail\log_fail_{}.log".format(general_path, today)

    return {'sqls_path': sqls_path, 'dataset_path': dataset_path, 'key_file': key_file, 'log_file': log_file,
            'log_fail_file': log_fail_file}


def main():
    directories = generate_directories()
    try:
        generate_log(directories['log_file'], 'Start process!')
        process = Processing(directories)
        process.process()
        generate_log(directories['log_file'], 'Finish process!')
    except Exception as fail:
        generate_log(directories['log_fail_file'], 'Except Interrupt System! fail: {}'.format(fail))


if __name__ == '__main__':
    main()
