# Purpose: Define functions to scrape info on concert events asynchronously.
# Using the list of URLs corresponding to concerts, we can get info from each
# using BeautifulSoup, and using asynchronous code to improve efficiency

# We carry out asynchronous web scraping by implementing application-level
# multiprocessing (vs process,thread level) because we want to scrape thousands
# of URLs. We use the asyncio and aiohttp modules for this.

import aiohttp
import asyncio
from bs4 import BeautifulSoup
import lxml.html
from aiohttp import ClientSession, ClientTimeout
import nest_asyncio
import boto3
from datetime import datetime
import pickle
import os, configparser
nest_asyncio.apply()

import getURLs

import pandas as pd

async def get_pages(session, url):
    async with session.get(url) as r:
        return await r.text()

async def get_tasks(session,urls):
    tasks = []
    for url in urls:
        task = asyncio.create_task(get_pages(session,url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results

async def main(urls):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
        data = await get_tasks(session,urls)
        return data
    
def get_ids(urls):
    event_ids = []
    for i in range(len(urls)):
        event_ids.append(urls[i].split("/")[-1])
    return event_ids

def parse(results):
    events = []
    for html in results:
        soup = BeautifulSoup(html,'lxml')
        # get perfomer, venue name, date, time, genre
        info = [soup.find_all('tr')[i].text for i in range(4)]
        # get concertful ranking
        info.append((soup.find(class_='aln')).text.strip('#'))
        events.append(info)
    return events

if __name__ == '__main__':
    urls = getURLs.get_list()
    results = asyncio.run(main(urls))
    total_data = parse(results)
    df = pd.DataFrame(total_data)
    df['concert_id']=get_ids(urls)
    pickled_df = pickle.dumps(df)
    
    this_folder = os.path.dirname(os.path.abspath(__file__))
    file_conf = os.path.join(this_folder, 'pipeline.conf')
    parser = configparser.ConfigParser()
    parser.read(file_conf)
    bucket_name = parser.get("aws_boto_credentials", "bucket_name")
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    concerts_key= 'concerts_df_{}.pkl'.format(datetime.now().strftime("%Y-%m-%d"))
    bucket.put_object(Key=concerts_key, Body=pickled_df)
