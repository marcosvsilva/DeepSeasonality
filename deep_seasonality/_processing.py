import pandas as pd
from datetime import date, datetime, timedelta
from _persistence import Persistence
from _holidays import Holidays


class Processing:
    def __init__(self, directories):
        self._directories = directories
        self._persistence = Persistence(directories['sqls_path'], directories['key_file'])
        self._holidays = []

    def verify_movement(self, product):
        params = {':id_company': product['id_company'], ':id_product': product['id']}
        quantity = self._persistence.sql_query('verify_movements', ['sales'], params)
        return int(quantity[0]['sales']) > 100

    def process_product(self, products):
        for product in products:
            if self.verify_movement(product):
                params = {':id_company': product['id_company'], ':id_product': product['id']}
                movements = self._persistence.sql_query('get_movements', ['date', 'sales', 'quantity'], params)
                list_dates, list_months, list_days, list_days_of_week, list_is_holiday = [], [], [], [], []
                list_quantities = []

                for movement in movements:
                    date_movement = datetime.strptime(movement['date'], "%Y-%m-%d")

                    list_range = [date_movement]
                    for number_range in range(1, 3):
                        list_range.append(date_movement+timedelta(days=number_range))

                    list_is_holiday.append(1 if any(x in list_range for x in self._holidays) else 0)
                    list_dates.append(movement['date']), list_days_of_week.append(date_movement.weekday())
                    list_months.append(str(date_movement.month)), list_days.append(str(date_movement.day))
                    list_quantities.append(movement['quantity'])

                df = pd.DataFrame({'date': list_dates, 'month': list_months, 'days': list_days,
                                   'week_day': list_days_of_week, 'is_holiday_ever': list_is_holiday,
                                   'quantity': list_quantities})

                df.to_csv('{}\\{}_{}.csv'.format(self._directories['dataset_path'], product['id_company'],
                                                 product['id']), index=None, header=True)

    def process(self):
        products_fields = ['id_company', 'id', 'code', 'name', 'balance']

        try:
            holidays = Holidays()
            
            list_holidays = holidays.get_holidays()
            for list_holiday in list_holidays:
                for holiday in list_holiday:
                    self._holidays.append(datetime.strptime(holiday['date'], '%d/%m/%Y'))

            companies = self._persistence.sql_query('get_companies', ['id'])
            for company in companies:
                param = {':id_company': company['id']}
                products = self._persistence.sql_query('get_products', products_fields, param)
                self.process_product(products)
        except Exception as fail:
            raise Exception('exception abort, fail: {}'.format(fail), True)
