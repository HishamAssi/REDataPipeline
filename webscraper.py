from typing import Dict

from bs4 import BeautifulSoup
import requests
from csv import writer
import time
import random
from lxml import etree as et
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


class HouseSigmaScraper:
    agent = {"User-agent": \
                 "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
                  (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"}
    states: Dict[str, str] = {'sold': 'justsold', 'listed': 'newlylisted'}

    def __init__(self, state='sold'):
        self.url = "https://housesigma.com/web/en/recommend/more/{}".format(self.states[state])
        chrome_options = Options()
        chrome_options.add_argument("user-data-dir=selenium")
        self.driver=webdriver.Chrome(options=chrome_options)


    def login(self):
        self.driver.get(self.url)
        time.sleep(5)
        self.driver.find_element(By.XPATH, "//*[@id=\"app\"]/div/div[1]/div[1]/div[2]/div[3]/a[1]").\
            click()
        time.sleep(2)
        self.driver.find_element(By.XPATH, "//*[@id=\"pane-email\"]/form/div[1]/div/div/input").\
            send_keys("")
        self.driver.find_element(By.XPATH, "//*[@id=\"pane-email\"]/form/div[2]/div/div/input"). \
            send_keys("")
        self.driver.find_element(By.XPATH, "//*[@id=\"login\"]/div[2]/div/div[3]/button").click()
        time.sleep(2)

    def get_data_list(self):
        self.driver.navigate().to(self.url)
        time.sleep(2)
        response = requests.get(test_url, headers=self.agent)
        soup = BeautifulSoup(response.text, 'lxml')
        print(soup)
        return et.HTML(str(soup))

    def get_data(self, listing_url):
        self.driver.navigate().to(listing_url)
        time.sleep(2)
        full_list=driver.find_element(By.CLASS_NAME, "item")
        return full_list



test_url = "https://housesigma.com/web/en/house/aD6p781zvPr3wRQr/6680-93-Hwy-County-Rd-Tay-L0K2E0-S5818021-S5818021-40343185"
scraper = HouseSigmaScraper('sold')
scraper.login()
time.sleep(10)
