import pandas as pd
from datetime import date, timedelta

from snowballstemmer.dutch_stemmer import lab0

from _persistence import Persistence


class Processing:
    def __init__(self, directories):
        self._directories = directories
        self._persistence = Persistence(directories['sqls_path'], directories['key_file'])

    def verify_moviment(self, product):
        params = {':id_company': product['id_company'], ':id_product': product['id']}
        quantity = self._persistence.sql_query('verify_moviment', ['quantity'], params)
        return int(quantity[0]['quantity']) > 100

    def process_product(self, products):
        for product in products:
            if self.verify_moviment(product):
                dt_end, dt_start, ldate, lquantity = date.today(), (date.today() - timedelta(days=730)), [], []
                ldays, aux = [], 0

                for sh_date in pd.date_range(start=dt_start, end=dt_end, freq='D'):
                    aux += 1
                    str_date = sh_date.strftime("%m-%d-%Y")
                    params = {':id_company': product['id_company'], ':id_product': product['id'], ':date': str_date}
                    quantity = self._persistence.sql_query('get_moviment', ['quantity'], params)
                    ldate.append(str_date), lquantity.append(quantity[0]['quantity']), ldays.append(aux)

                df = pd.DataFrame({'date': ldate, 'days': ldays, 'quantity': lquantity})
                df.to_csv('{}\\{}_{}.csv'.format(self._directories['dataset_path'], product['id_company'],
                                                 product['id']), index=None, header=True)

    def process(self):
        companies_fields = ['id', 'doc', 'name']
        products_fields = ['id_company', 'id', 'code', 'name', 'balance']

        try:
            data = {}
            companies = self._persistence.sql_query('get_companies', companies_fields)
            for company in companies:
                param = {':id_company': company['id']}
                products = self._persistence.sql_query('get_products', products_fields, param)
                self.process_product(products)
        except Exception as fail:
            raise Exception('exception abort, fail: {}'.format(fail), True)
