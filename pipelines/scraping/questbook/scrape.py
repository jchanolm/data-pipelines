from ..helpers import Scraper
import logging 
import time
import os 
import json 
import datetime 
import re
import requests as requests
import pandas as pd 

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options

class QuestbookScraper(Scraper):
    def __init__(self, bucket_name="questbook", load_data=False):
        super().__init__(bucket_name=bucket_name, load_data=load_data)
        self.base_url = "https://new.questbook.app/"
        self.headers = {
            'Content-Type': 'application/json'
        }
        options = Options()
        options.set_preference("general.useragent.override", "Firefox: Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0")
        self.driver = webdriver.Firefox(options=options)

    def get_initiative_links(self):

        driver = self.driver 
        driver.get('https://new.questbook.app/')
        time.sleep(10)
    


    def run(self):
        self.save_metadata()
        self.save_data()

if __name__ == "__main__":
    scraper = QuestbookScraper()
    scraper.run()
