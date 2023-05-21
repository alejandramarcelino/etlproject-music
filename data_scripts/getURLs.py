# We first get links of webpages associated with each state.
# Then among each state, we gather the links of the concerts listed.
# The latter is the list of URLs we want, since we can get data from each URL
# consisting of information on each concert

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep
from random import randint
import numpy as np

def get_list():
    ignored_exceptions=(NoSuchElementException,StaleElementReferenceException)
    with webdriver.Safari() as driver:
        driver.get('https://concertful.com/area/united-states/')

        states = driver.find_elements(By.CSS_SELECTOR, "a[href*='/area/united-states/'][href$='/']")
        links_states = [state.get_attribute('href') for state in states]

        url_array = []
        for i in range(len(links_states)):
            driver.get(links_states[i])
            sleep(randint(5,15))
            try:
                total_pgs = driver.find_element(By.XPATH, "//div[@class='buttons_counter']/span")
                total_pgs = int(total_pgs.text.split(" ")[-1])
                pages = np.arange(1, total_pgs+1, 1)
                for page in pages:
                    driver.get(f'{links_states[i]}?page={page}')
                    sleep(randint(5,10))
                    concert_events = wait(driver, 20,ignored_exceptions=ignored_exceptions)\
                            .until(EC.presence_of_all_elements_located((By.CSS_SELECTOR,\
                                                        "a[href*='/event/']")))
                    urls = [concert.get_attribute('href') for concert in concert_events]
                    url_array.extend(urls)
            # if concerts for a state fits in one page
            except Exception:    
                concert_events = wait(driver, 20,ignored_exceptions=ignored_exceptions)\
                                .until(EC.presence_of_all_elements_located((By.CSS_SELECTOR,\
                                                            "a[href*='/event/']")))
                urls = [concert.get_attribute('href') for concert in concert_events]
                url_array.extend(urls)
    return url_array
