import json
import logging
import re
import threading
import time
from queue import Queue
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Please provide URL\'s list
SEEDS_URL = ['https://www.imdb.com/', 'https://www.tel-aviv.gov.il/Residents/Transportation/Pages/Appeal.aspx']
NUM_OF_THREADS = 10
TIME_DURATION_IN_SECONDS = 2000

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s', )


class VisitedUrl:
    """This class will hold and manage all the urls that the crawler visited"""
    def __init__(self):
        self.__data_base = {}

    def add_url_to_visited_urls(self, url: str) -> None:
        if url and url not in self.__data_base:
            self.__data_base.update({url: 'placeholder'})

    def check_if_url_in_visited_urls(self, url) -> None:
        if url not in self.__data_base:
            history.add_url_to_filter(url)

    def get_visited_urls(self) -> dict:
        return self.__data_base


class UrlFilter:
    """This class will manage all the urls Before they enter the Queue for processing"""
    def __init__(self):
        self.__dict = {}

    def add_url_to_filter(self, url: str) -> None:
        if url not in self.__dict:
            self.__dict.update({url: 'place holder'})
            urls_waiting_for_visit_queue.put(url)

    def get_history_urls(self) -> dict:
        return self.__dict


class EmailManger:
    """This class will manage all the emails that the app is gathering"""
    def __init__(self):
        self.__db = {}
        self.__aggregate_emails_set = set()

    def add_email(self, url: str, emails_set: set):
        self.__db.update({url: emails_set})
        self.__aggregate_emails_set = set.union(self.__aggregate_emails_set, emails_set)
        self._write_emails_to_a_file(self.__aggregate_emails_set)

    def _write_emails_to_a_file(self, email_set):
        results = dict.fromkeys(email_set, 0)
        with open('emails.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)

    def get_emails_dictionary(self):
        return self.__db

    def get_all_emails(self):
        return self.__aggregate_emails_set


class ConsumerThread(threading.Thread):
    """"
    This is the worker thread function.
    It processes items in the queue one after
    another.  These daemon threads go into an
    infinite loop, and only exit when
    the main thread ends.
    """

    def __init__(self, target=None, name=None):
        super(ConsumerThread, self).__init__()
        self.target = target
        self.name = name
        return

    def run(self):
        time_duration = TIME_DURATION_IN_SECONDS
        time_start = time.time()

        while time.time() < time_start + time_duration:
            if not urls_waiting_for_visit_queue.empty():

                try:
                    item = urls_waiting_for_visit_queue.get()
                    logging.debug('Getting ' + str(item)
                                  + ' : ' + str(urls_waiting_for_visit_queue.qsize()) + ' items in queue')

                    html = self._download_url(item)

                    visited.add_url_to_visited_urls(item)
                    for url in self._get_data_from_html_page(item, html):
                        visited.check_if_url_in_visited_urls(url)
                    self._get_emails_from_page(item, html)
                    logging.debug(email_manager.get_all_emails())
                    logging.debug(f'number of visited urls: {len(visited.get_visited_urls())}')

                except:
                    logging.error('Error with URL check!')
                finally:
                    urls_waiting_for_visit_queue.task_done()
        exit(0)

    def _download_url(self, url):
        try:
            return requests.get(url).text
        except:
            logging.error('failed to download url')

    def _get_data_from_html_page(self, item, html):
        try:
            soup = BeautifulSoup(html, 'lxml')

            for link in soup.find_all('a'):
                path = link.get('href')
                if path and path.startswith('/'):
                    path = urljoin(item, path)
                yield path
        except:
            pass

    def _get_emails_from_page(self, item: str, html: str) -> None:
        soup = BeautifulSoup(html, 'lxml')
        new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", soup.text, re.I))
        if new_emails:
            email_manager.add_email(item, new_emails)


if __name__ == '__main__':
    urls_waiting_for_visit_queue = Queue()
    visited = VisitedUrl()
    email_manager = EmailManger()
    history = UrlFilter()

    for url in SEEDS_URL:
        urls_waiting_for_visit_queue.put(url)

    threads = []

    for num in range(10):
        consumer = ConsumerThread(name=f'consumer {num} ')
        consumer.daemon = True
        consumer.start()
        threads.append(consumer)
    urls_waiting_for_visit_queue.join()
