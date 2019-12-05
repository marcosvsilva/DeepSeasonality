import pandas as pd
import datetime


class NeuralAnalysis:
    def __init__(self):
        self._data = []
        self._params = {}

        self.first_year = None
        self.second_year = None

    def _clear_dates(self):
        self._data.clear()
        self._params.clear()
        self.first_year = None
        self.second_year = None

    def _analysis(self, begin, end):
        data_first = self.first_year[self.first_year.date >= begin - datetime.timedelta(days=365)]
        data_first = data_first[data_first.date < end - datetime.timedelta(days=365)]

        data_second = self.second_year[self.second_year.date >= begin]
        data_second = data_second[data_second.date < end]

        quantity_first, quantity_second = data_first.quantity, data_second.quantity
        sum_first, sum_second = float(quantity_first.sum()), float(quantity_second.sum())

        validate = (max([sum_first, sum_second]) - min([sum_first, sum_second]))
        validate = validate < (max([sum_first, sum_second]) * 0.5)
        quantity = ((sum_first * 1.2) + (sum_second * 1.2)) / 2

        if validate:
            self._data.append({**self._params, **{':begin_month': begin.month, ':begin_date': begin.day,
                                                  ':end_month': end.month, ':end_day': end.day, ':quantity': quantity}})

    def run_analysis(self, params, data):
        try:
            self._clear_dates()
            self._params = params

            data = self._convert_date(data)
            rows, median = int(data.shape[0] / 2), data[data.quantity > 0].quantity.mean()

            first_year, second_year = data[:rows], data[data.shape[0]-rows:]
            self.first_year = first_year[first_year.quantity > median]
            self.second_year = second_year[second_year.quantity > median]

            date_list = pd.date_range(end=datetime.date.today(), periods=52, freq='W').tolist()
            begin = date_list[0]
            for end in date_list:
                self._analysis(begin, end)
                begin = end

            return self._data
        except Exception as fail:
            raise Exception('Exception neural analysis, fail: {}'.format(fail), True)

    @staticmethod
    def _convert_date(data):
        new_date = data.date.astype('datetime64')
        data.drop('date', axis=1)
        data['date'] = new_date
        return data
