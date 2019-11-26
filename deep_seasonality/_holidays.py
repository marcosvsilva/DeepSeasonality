import requests
import json
from datetime import date


def get_api_holidays(year):
    protocol = 'https'
    link = 'api.calendario.com.br/'
    year = year
    state = 'GOIAS'
    city = 'GOIANIA'
    token = 'bWFyY29zLnYuc2lsdmFAbGl2ZS5jb20maGFzaD0xMjE3NDg1OQ'

    try:
        url_request = '{}://{}?json=true&ano={}&estado={}&cidade={}&token={}'.format(protocol, link, year,
                                                                                     state, city, token)

        response = requests.Session().get(url_request)

        if response.status_code == 200:
            response.encoding = 'utf-8'
            return json.loads(response.text)
        else:
            return ''
    except Exception as fail:
        raise Exception('Exception api request, fail: {}'.format(fail))


class Holidays:
    def __init__(self):
        current_year, last_year, last_last_year = date.today().year, date.today().year - 1, date.today().year - 2
        self.holidays_current_year = get_api_holidays(current_year)
        self.holidays_last_year = get_api_holidays(last_year)
        self.holidays_last_last_year = get_api_holidays(last_last_year)

    def get_holidays(self):
        return [self.holidays_current_year, self.holidays_last_year, self.holidays_last_last_year]
