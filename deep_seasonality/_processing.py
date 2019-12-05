import pandas as pd
from datetime import datetime, timedelta
from _persistence import Persistence
from _holidays import Holidays
from _neural_analysis import NeuralAnalysis
from _log import generate_log


class Processing:
    def __init__(self, directories):
        self._directories = directories
        self._persistence = Persistence(directories['sqls_path'], directories['key_file'])
        self._neural_analysis = NeuralAnalysis()
        self._holidays = []

    def _verify_movement(self, product):
        params = {':id_company': product['id_company'], ':id_product': product['id']}
        quantity = self._persistence.sql_query('verify_movements', ['sales'], params)
        return int(quantity[0]['sales']) > 100

    def _process_product(self, products):
        for product in products:
            if self._verify_movement(product):
                generate_log(self._directories['log_file'], 'Product {} have qualify movement!'.format(product['id']))

                params = {':id_company': product['id_company'], ':id_product': product['id']}
                movements = self._persistence.sql_query('get_movements', ['date', 'sales', 'quantity'], params)
                list_dates, list_months, list_days, list_days_of_week, list_is_holiday = [], [], [], [], []
                list_quantities = []

                for movement in movements:
                    date_movement = datetime.strptime(movement['date'], "%Y-%m-%d")

                    list_range = [date_movement]
                    for number_range in range(1, 3):
                        list_range.append(date_movement + timedelta(days=number_range))

                    list_is_holiday.append(1 if any(x in list_range for x in self._holidays) else 0)
                    list_dates.append(movement['date']), list_days_of_week.append(date_movement.weekday())
                    list_months.append(str(date_movement.month)), list_days.append(str(date_movement.day))
                    list_quantities.append(movement['quantity'])

                data = pd.DataFrame({'date': list_dates, 'month': list_months, 'days': list_days,
                                     'week_day': list_days_of_week, 'is_holiday_ever': list_is_holiday,
                                     'quantity': list_quantities})

                seasonalities = self._neural_analysis.run_analysis(params, data)
                if len(seasonalities) > 0:
                    self._persistence.sql_update('set_seasonality', seasonalities)
                    generate_log(self._directories['log_file'],
                                 'Update seasonalities product: {}!'.format(product['id']))

    def process(self):
        try:
            generate_log(self._directories['log_file'], 'Clean seasonality!')
            self._persistence.sql_execute('clean_seasonality')

            generate_log(self._directories['log_file'], 'Find holidays!')
            holidays = Holidays()

            list_holidays = holidays.get_holidays()
            for list_holiday in list_holidays:
                for holiday in list_holiday:
                    self._holidays.append(datetime.strptime(holiday['date'], '%d/%m/%Y'))

            companies = self._persistence.sql_query('get_companies', ['id'])
            for company in companies:
                generate_log(self._directories['log_file'], 'Find products company: {}!'.format(company['id']))

                fields, params = ['id_company', 'id'], {':id_company': company['id']}
                products = self._persistence.sql_query('get_products', fields, params)
                self._process_product(products)
        except Exception as fail:
            raise Exception('Exception abort, fail: {}'.format(fail), True)
