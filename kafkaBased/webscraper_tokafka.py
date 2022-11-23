from typing import Dict
import requests
from bs4 import BeautifulSoup
import lxml.html
import datetime
import requests
import time
import random
import boto3
import re
from lxml import etree as et
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
from login_credentials import username,password
from kafka_producer import kafka_producer



class HouseSigmaScraper:

    states: Dict[str, str] = {'sold': 'justsold', 'listed': 'newlylisted'}

    def __init__(self, state='sold', timeout=5, topic="housesigmascraper"):
        """Creates the webdriver for scraping HouseSigma.com and a kafka producer.

        :param state: sold or listed - decides whether to scrape the site for sold data or newly listed data.
        :param timeout: timeout for page loading, maximum of 5 seconds.
        :param topic: topic name
        """
        self.url = "https://housesigma.com/web/en/recommend/more/{}".format(self.states[state])
        chrome_options = Options()
        # Allows cookies to be stored so login is not required every time.
        chrome_options.add_argument("user-data-dir=selenium")
        self.driver=webdriver.Chrome(options=chrome_options)
        self.state = state
        self.timeout = timeout
        conf = kafka_producer.\
            read_ccloud_config("/Users/hisham/PycharmProjects/pythonProject/venv/proj/credentials/KafkaDevConfig.properties")
        self.producer = kafka_producer(conf)
        self.topic = topic
        # Create topic only if it doesn't exist
        kafka_producer.create_topic(conf, topic)

    def login(self):
        """
        login if needed
        login_credentials.py should be in the same folder, containing two variables, username and password.

        :return: None - the webdriver should be logged in afterwards.
        """
        try:
            print("logging in")
            self.driver.get(self.url)
            self.driver.find_element(By.XPATH, "//*[@id=\"app\"]/div/div[1]/div[1]/div[2]/div[3]/a[1]").\
                click()
            time.sleep(2)
            self.driver.find_element(By.XPATH, "//*[@id=\"pane-email\"]/form/div[1]/div/div/input").\
                send_keys(username)
            self.driver.find_element(By.XPATH, "//*[@id=\"pane-email\"]/form/div[2]/div/div/input"). \
                send_keys(password)
            self.driver.find_element(By.XPATH, "//*[@id=\"login\"]/div[2]/div/div[3]/button").click()
        except:
            print("Login not needed")


    def get_all_data(self):
        """
        gets a list of all links to properties whether sold or listed, iterates through it, executing get_data to
        obtain all the information.
        :return: None - JSON info for the links is written to Kafka.
        """
        print("getting data")
        self.driver.get(self.url)
        try:
            element_present = EC.presence_of_element_located((By.CLASS_NAME, 'el-card__body'))
            WebDriverWait(self.driver, self.timeout).until(element_present)
        except TimeoutException:
            print("Timed out waiting for page to load")
        soup = BeautifulSoup(self.driver.page_source, 'lxml')

        all_links = soup.select('div.el-card__body')
        print("links found: " + str(len(all_links)))
        domain = "https://housesigma.com"
        for div in all_links:
            url = domain+div.find('a')['href']
            self.producer.produce_data(json.dumps(self.get_data(url)), url, self.topic)
            # all_listings.append(self.get_data(domain+div.find('a')['href']))
        self.producer.flush()

    def get_data(self, listing_url):
        """
        gets all data for a specific listing.
        :param listing_url: housesigma url for a specific address
        :return: Dict - all details about the url.
        """
        print("getting data for: " + listing_url)
        self.driver.get(listing_url)
        try:
            element_present = EC.presence_of_element_located((By.CLASS_NAME, 'item'))
            WebDriverWait(self.driver, self.timeout).until(element_present)
        except TimeoutException:
            print("Timed out waiting for page to load")
        soup=BeautifulSoup(self.driver.page_source, 'lxml')
        items = {"Link": listing_url}

        for div in soup.select('div.item'):
            if(div.find("h2") and div.find("span")):
                key = re.sub('[^0-9a-zA-Z_]+', '', div.find("h2").string)
                items[key] = div.find("span").string
        return items


if __name__ == "__main__":
    scrape = HouseSigmaScraper()
    scrape.get_all_data()



