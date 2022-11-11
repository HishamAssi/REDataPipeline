from typing import Dict
import requests
from bs4 import BeautifulSoup
import lxml.html
import requests
from csv import writer
import time
import random
from lxml import etree as et
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import json
import browser_cookie3
from login_credentials import username,password



class HouseSigmaScraper:
    states: Dict[str, str] = {'sold': 'justsold', 'listed': 'newlylisted'}

    def __init__(self, state='sold'):
        self.url = "https://housesigma.com/web/en/recommend/more/{}".format(self.states[state])
        chrome_options = Options()
        chrome_options.add_argument("user-data-dir=selenium")
        self.driver=webdriver.Chrome(options=chrome_options)
        self.state = state


    def login(self):
        self.driver.get(self.url)
        time.sleep(5)
        self.driver.find_element(By.XPATH, "//*[@id=\"app\"]/div/div[1]/div[1]/div[2]/div[3]/a[1]").\
            click()
        time.sleep(2)
        self.driver.find_element(By.XPATH, "//*[@id=\"pane-email\"]/form/div[1]/div/div/input").\
            send_keys(username)
        self.driver.find_element(By.XPATH, "//*[@id=\"pane-email\"]/form/div[2]/div/div/input"). \
            send_keys(password)
        self.driver.find_element(By.XPATH, "//*[@id=\"login\"]/div[2]/div/div[3]/button").click()
        time.sleep(2)

    def get_data_list(self):
        self.driver.get(self.url)
        time.sleep(5)
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        all_links = soup.select('div.el-card__body')
        all_listings = []
        domain = "https://housesigma.com"
        for div in all_links:

            print(div.find('a')['href'])
            all_listings.append(self.get_data(domain+div.find('a')['href']))

        return all_listings

    def get_data(self, listing_url):

        self.driver.get(listing_url)
        time.sleep(5)
        soup=BeautifulSoup(self.driver.page_source, 'lxml')
        items = {"link": listing_url}

        for div in soup.select('div.item'):
            if(div.find("h2") and div.find("span")):
                items[div.find("h2").string] = div.find("span").string
            print(div)
            print(div.find("h2"))
            print(div.find("span"))
            # print(div.find())
        return items

    def write_data_to_file(self, data):
        with open("./data/extracted_{}.json".format(self.state), "w") as output:
            for item in data:
                json.dump(json.dumps(item), output)
                output.write("\n")







# test_url = "https://housesigma.com/web/en/house/aD6p781zvPr3wRQr/6680-93-Hwy-County-Rd-Tay-L0K2E0-S5818021-S5818021-40343185"
scraper = HouseSigmaScraper('sold')
