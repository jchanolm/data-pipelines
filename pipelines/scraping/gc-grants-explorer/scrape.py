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

class GcExplorerScraper(Scraper):
    def __init__(self, bucket_name="gc-grants-explorer", load_data=False):
        super().__init__(bucket_name=bucket_name, load_data=load_data)
        self.base_url = "https://explorer.gitcoin.co/#/rounds?orderBy=NATURAL&status=&type=&network=" ## removes filters to get all rounds
        self.headers = {
            'Content-Type': 'application/json'
        }
        options = Options()
        options.set_preference("general.useragent.override", "Firefox: Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0")
        self.driver = webdriver.Firefox(options=options)
    ## steps


    def get_all_hrefs(self, selenium_driver, filtr=None):
        links = selenium_driver.find_elements(By.TAG_NAME, 'a')
        if not links:
            print("No links found")
            return None 
        else:
            try:
                hrefs = [i.get_attribute('href') for i in links]
                final_hrefs = [i for i  in hrefs if filtr in i]
                if final_hrefs:
                    return final_hrefs
                else:
                    return None 
            except Exception as e:
                logging.error(f"Error retrieving hrefs...")
                pass 


    def get_twitters(self, urls):
        twitters = []
        for url in urls:
            if "x.com" in url or 'twitter.com' in url:
                twitters.append(url.split('/')[3])
        return list(set(twitters))
    
    def get_githubs(self, urls):
        githubs = []
        for url in urls:
            if "github.com" in url:
                githubs.append(url.split('/')[3])
        return list(set(githubs))
    
    def get_mirrors(self, urls):
        mirrors = []
        for url in urls:
            if "mirror.xyz" in url: 
                mirrors.append('/'.join(url.split('/')[0:3]))
        return list(set(mirrors)) 

    def get_dunes(self, urls):
        dunes = []
        for url in urls:
            if "dune.com" in url: 
                dunes.append(url.split('/')[3])
        return list(set(dunes)) 
    
    def get_other_websites(self, urls):
        websites = []
        for url in urls:
            if not "github" in urls and not "x.com" in urls and not 'twitter.com' and not "gitcoin" in urls:
                websites.append(url)
        return list(set(urls))
    

    
    def gather_rounds(self):
        logging.info("Gathering Round URLs....")
        driver = self.driver
        driver.get(self.base_url)
        time.sleep(2.5)
        try: 
            round_hrefs = self.get_all_hrefs(driver, "0x")
            if round_hrefs: 
                return round_hrefs
        except Exception as e:
            logging.error(f"No round URLs found: {e}")

    def gather_submissions(self, round_url):
        logging.info("Gathering submission URLs...")
        driver = self.driver 
        driver.get(round_url)
        time.sleep(2.5)
        try: 
            round_hrefs = self.get_all_hrefs(driver, '0x')
            if round_hrefs:
                return round_hrefs 
        except Exception as e:
            logging.error(f"No submission URLs found with error: {e}")
            
    def get_round_info(self, round_url):
        logging.info(f"Collecting round information from {round_url}...")
        driver = self.driver
        driver.get(round_url)
        time.sleep(2.5)
        round_dict = {}
        try:
            round_dict['url'] = round_url
            title_element = driver.find_element(By.CSS_SELECTOR, '[data-testid="round-title"]')
            round_dict['title'] = title_element.text if title_element else None
            round_type_element = driver.find_element(By.CSS_SELECTOR, '[data-testid="round-badge"]')
            round_dict['roundType'] = round_type_element.text if round_type_element else None
            date_elements = driver.find_elements(By.CSS_SELECTOR, "span.mr-1")
            round_dict['startDt'] = date_elements[1].text.replace('/', '-') if date_elements[1] else None
            round_dict['endDt'] = date_elements[3].text.replace('/', '-') if date_elements[3] else None
            chain_img = driver.find_element(By.CSS_SELECTOR, "img[alt='Round Chain Logo']")
            round_dict['chainImg'] = chain_img.text if chain_img else None
            chain_name_element = driver.find_element(By.XPATH, "//img[@alt='Round Chain Logo']/following-sibling::span")
            round_dict['chain'] = chain_name_element.text if chain_name_element else None
            matching_info_element = driver.find_element(By.CSS_SELECTOR, "p.text-1xl.mb-4")
            round_dict['matchingInfo'] = matching_info_element.text if matching_info_element else None 
            paragraph = driver.find_element(By.CSS_SELECTOR, 'p.text-1xl.mb-4.overflow-x-auto')
            round_dict['text'] = paragraph.text if paragraph else None
            print(round_dict)
            return round_dict
        except Exception as e:
            logging.info(f"Error {e} for retrieving info for round url: {round_url}")
            pass 

    
    def gather_submission_info(self, submission_url):
        try:
            driver = self.driver
            driver.get(submission_url)
            time.sleep(2.5)
            hrefs = self.get_all_hrefs(driver, submission_url)
            page_source = driver.page_source
            pattern = r'\b\w+\.eth\b'
            ens_name_match = re.search(pattern, page_source)
            ens_name = ens_name_match.group(0) if ens_name_match else None
            twitters = self.get_twitters(hrefs)
            githubs = self.get_githubs(hrefs)
            dunes = self.get_dunes(hrefs)
            mirrors = self.get_mirrors(hrefs)
            paragraphs = driver.find_elements(By.CSS_SELECTOR, "p.text-md")
            description = ' '.join([paragraph.text for paragraph in paragraphs]) if paragraphs else None
            anchor_element = driver.find_element(By.CSS_SELECTOR, "a.text-blue-200.hover\\:underline")
            main_website = anchor_element.get_attribute('href') if anchor_element else None
            submission_dict = {}
            project_name = driver.find_element(By.CSS_SELECTOR, 'h1.text-4xl')
            date_div = driver.find_element(By.XPATH, "//div[contains(text(), 'Created on:')]")
            date_txt = date_div.text 
            formatted_date = None
            if date_txt:
                date_pattern = r"Created on: (\w+ \d{1,2}(?:th|rd|nd|st), \d{4})"
                date_match = re.search(date_pattern, date_txt)
                if date_match:
                    date_str = date_match.group(1)
                    day_suffix_pattern = r"(\d{1,2})(?:th|rd|nd|st)"
                    date_str = re.sub(day_suffix_pattern, r"\1", date_str)
                    try:
                        date_obj = datetime.datetime.strptime(date_str, "%B %d, %Y")
                        formatted_date = date_obj.strftime("%Y-%m-%d")
                    except ValueError as e:
                        logging.error(f"Date conversion error: {e}")
            submission_dict['projectName'] = project_name.text if project_name else None 
            submission_dict['mainWebsite'] = main_website if main_website else None 
            submission_dict['twitters'] = twitters if twitters else None 
            submission_dict['githubs'] = githubs if githubs else None
            submission_dict['dunes'] = dunes if dunes else None
            submission_dict['mirrors'] = mirrors if mirrors else None 
            submission_dict['ensName'] = ens_name if ens_name else None 
            submission_dict['description'] = description if description else None
            submission_dict['publishedDt'] = formatted_date if formatted_date else None  
            print(submission_dict)
            return submission_dict
        except Exception as e:
            print(f"error {e}")

    def scrape_git_explorer(self):
        completed_dicts = []
        round_urls = self.gather_rounds()
        if not round_urls:
            logging.info("No round URLs found. Finishing execution.")
            return
        logging.info(f"Scraping {len(round_urls)} rounds...")
        for round_url in round_urls:
            round_info = self.get_round_info(round_url)
            submission_urls = self.gather_submissions(round_url)
            if not submission_urls:
                logging.info(f"No submission URLs found for round {round_url}. Continuing to next round.")
                continue
            submission_dicts_list = []
            for sub in submission_urls: 
                submission = self.gather_submission_info(sub)
                submission_dicts_list.append(submission)
            round_info['submissions'] = submission_dicts_list
            completed_dicts.append(round_info)
        if not completed_dicts:
            logging.info("No data collected. Finishing execution.")
            return
        self.data['data'] = completed_dicts


    def run(self):
        self.scrape_git_explorer()
        self.save_metadata()
        self.save_data()



if __name__ == "__main__":
    scraper = GcExplorerScraper()
    scraper.run()




