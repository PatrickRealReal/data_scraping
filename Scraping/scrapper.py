import http
import logging
import ssl
import time
from abc import ABC

import requests
import pandas as pd
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException


# general class of scraping
class scrapper:
    ## define class attributes here (below is just an example)
    _table_name = ''
    _url = 'https://www.enecho.meti.go.jp/statistics/electric_power/ep002/xls/2022/3-1-2022.xlsx'

    ## define instance initialization here (below is just an example)
    def __init__(self):
        pass

    @property
    def table_name(self):
        return self._table_name

    def testing(self):
        http.client.HTTPConnection.debuglevel = 1
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True
        requests.get(self.url)

    def data(self):
        '''
        Main function for data scraping
        :param sd: datetime, starting datetime for scraping
        :param ed: datetime, ending datetime for scraping

        :return: Pandas.DataFrame, final dataframe
        '''
        ssl._create_default_https_context = ssl._create_unverified_context
        df = pd.read_csv(self._url, encoding='ISO-8859-1')
        return None



solution = scrapper()
solution.data()

# Feedback: if we could read it to a variable...and do sth based on it.

# you may also want to create subclasses, like for different Area, ProductType, or Baseload/Peakload
