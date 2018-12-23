from Memoized import Memoized
from urllib import parse
from enum import Enum
import requests
import requests_oauthlib
import datetime
import os
import json
import logging
import time
import math
import pickle
from multiprocessing import Event, Process, SimpleQueue, Pipe
from HBase import HBaseClient

STREAM_BASE_URL = 'https://stream.twitter.com/1.1/statuses/filter.json'
DOWNLOAD_BASE_URL = 'https://api.twitter.com/1.1/search/tweets.json'


class RateLimitError(Exception):
    pass


class NoValue(Enum):
    def __repr__(self):
        return '<%s.%s>' % (self.__class__.__name__, self.name)


class ResultType(Enum):
    MIXED = 'mixed'
    RECENT = 'recent'
    POPULAR = 'popular'


class Worker(object):

    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_secret: str,
                 base_service_url: str = None, name: str = None):
        self.max_id = None
        self._session = requests.session()
        self._oauth_token = self._get_oauth(consumer_key, consumer_secret, access_token, access_secret)
        self._base_service_url = base_service_url
        self._name = name

    @Memoized
    def _get_oauth(self, consumer_key: str, consumer_secret: str, access_token: str, access_secret: str):
        return requests_oauthlib.OAuth1(consumer_key, consumer_secret, access_token, access_secret)

    def _get_query_string(self, terms_, max_id: int = None):
        raise NotImplementedError("_get_query_string needs to be defined by concrete classes of Worker.")

    def _get_tweets(self, url, terms_, q):
        raise NotImplementedError("_get_tweets needs to be defined by concrete classes of Worker.")

    @property
    def base_service_url(self):
        return self._base_service_url

    @base_service_url.setter
    def base_service_url(self, value: bool):
        self._base_service_url = value

    @property
    def session(self):
        return self._session

    @session.setter
    def session(self, value: bool):
        self._session = value

    @property
    def oauth_token(self):
        return self._oauth_token

    @oauth_token.setter
    def oauth_token(self, value: bool):
        self._oauth_token = value

    @Memoized
    def process(self, term: str, q):
        url = self.base_service_url + '?' + self._get_query_string(term, max_id=self.max_id)
        return self._get_tweets(url, term, q)

    @Memoized
    def _encode(self, params: dict):
        for k, v in params.items():
            if v is not None:
                params[k] = parse.quote_plus(v)
        return params


class Downloader(Worker):

    def __init__(self, consumer_key: str, consumer_secret: str, access_token: str, access_secret: str,
                 base_service_url: str = None, name: str = None):
        super().__init__(consumer_key=consumer_key, consumer_secret=consumer_secret, access_token=access_token,
                         access_secret=access_secret,
                         base_service_url=base_service_url or DOWNLOAD_BASE_URL, name=name)
        self.max_id = None

    @Memoized
    def _get_query_string(self, term: str, geocode: str = None, lang: str = None, locale: str = None,
                          result_type: ResultType = ResultType.MIXED, count: int = 100,
                          until: datetime.date = None, since_id: int = None, max_id: int = None,
                          include_entities: bool = True, no_retweets=True):
        if no_retweets:
            term += ' -filter:retweets'
        params = self._encode({
            'q': term,
            'geocode': geocode,
            'lang': lang,
            'locale': locale,
            'result_type': result_type.value,
            'count': str(count),
            'until': until.strftime('%Y-%m-%d') if until else None,
            'since_id': str(since_id) if since_id else None,
            'max_id': str(max_id) if max_id else None,
            'include_entities': str(include_entities).lower(),
        })
        return '&'.join(['='.join([k, v]) for k, v in params.items() if v is not None])

    def _get_tweets(self, url, term, q):

        tweets = []
        q.get()
        logging.info('{} got a signal to download'.format(self._name))
        response = self.session.get(url, auth=self.oauth_token)
        # response = None
        backoff_time = 1
        if response is not None:
            if response.status_code == 429:
                logging.warning('Rate limited, please wait up to 15 minutes.  See '
                                'https://developer.twitter.com/en/docs/tweets/search/api-reference/'
                                'get-search-tweets.html'
                                'for details.')
                time.sleep(backoff_time)
                backoff_time = backoff_time * 2
                if backoff_time > (16 * 60):
                    raise RateLimitError("Rate limited for more than 16 minutes, when rate limit window is 15 minutes."
                                         "Something's wrong.")
                tweets.extend(self.process(term, q))
            try:
                tweets.extend(json.loads(response.text)['statuses'])
            except KeyError as e:
                logging.error(e)
                return []
            if tweets and len(tweets) == 100:
                self.max_id = min([status['id'] for status in tweets]) - 1
                new_tweets = self.process(term, q)
                tweets.extend(new_tweets)
        else:
            return self._get_tweets(url, term, q)
        return tweets


def run_downloader(term, consumer_key, consumer_secret, access_token, access_secret, q, name,
                   pipe=None, hbase_client=None, hbase_table='tweets', category=None):
    w = Downloader(consumer_key, consumer_secret, access_token, access_secret, None, name)

    ret = w.process(term, q)
    logging.warning('Worker complete, found {} tweets for {}'.format(len(ret), name))
    for tweet in ret:
        tweet['category'] = category
    hbase_client.write_tweets_to_hbase(ret, hbase_table)
    if pipe:
        pipe.send(ret)
        pipe.close()
    else:
        return ret


def rate_limit(complete, q, sleep_time):
    def is_rate_limited():
        try:
            status = json.loads(
                session.get('https://api.twitter.com/1.1/application/rate_limit_status.json?resources=search',
                            auth=token).text)
            remaining = status['resources']['search']['/search/tweets']['remaining']
        except KeyError:
            logging.warning('Bad response to rate limit call')
            return True
        if remaining <= 1:
            reset_time = status['resources']['search']['/search/tweets']['reset']
            logging.info(status)
            seconds = math.ceil((datetime.datetime.fromtimestamp(reset_time) - datetime.datetime.now()).total_seconds())
            logging.warning('Rate-limited, resets in {} seconds'.format(seconds))
            if seconds > 0:
                return True
        return False

    token = requests_oauthlib.OAuth1(os.environ['TWITTER_CONSUMER_KEY'], os.environ['TWITTER_CONSUMER_SECRET'],
                                     os.environ['TWITTER_ACCESS_TOKEN'], os.environ['TWITTER_ACCESS_SECRET'])
    session = requests.session()
    while True:
        if complete.is_set():
            return True
        if is_rate_limited():
            time.sleep(10)
            continue
        else:
            logging.info('Allowing request')
            try:
                q.put(True)
            except ValueError as e:
                logging.warning(e)
            # Sleep until releasing next so that we don't hit the 180 request / 15 min rate limit
            time.sleep(sleep_time)


def build_query_strings(terms_):
    strings = []
    q = ''
    for term in terms_:
        new_q = '(' + term + ')'
        if len(parse.quote_plus(q + ' OR ' + new_q)) > 50:
            strings.append(q)
            q = new_q
            continue
        if q == '':
            q = new_q
        else:
            q += ' OR ' + new_q
    if q != '':
        strings.append(q)
    return strings


def run_downloaders(terms_, consumer_key, consumer_secret, access_token, access_secret, pipe: Pipe = None,
                    category: str = None, hbase_client: HBaseClient = None, hbase_table: str = 'tweets',
                    sleep_time: float = 5.1):
    q = SimpleQueue()
    complete = Event()
    strings = build_query_strings(terms_)
    processes = []
    pipes = []
    for term in strings:
        parent_conn, child_conn = Pipe()
        p = Process(target=run_downloader, args=(term,), kwargs={
            'consumer_key': consumer_key,
            'consumer_secret': consumer_secret,
            'access_token': access_token,
            'access_secret': access_secret,
            'q': q,
            'name': term,
            'pipe': child_conn,
            'hbase_client': hbase_client,
            'hbase_table': hbase_table,
            'category': category,
        })
        processes.append(p)
        pipes.append(parent_conn)
        p.start()
    rate_limiter = Process(target=rate_limit,
                           args=(complete, q, sleep_time))
    rate_limiter.start()

    tweets = []
    while pipes:
        for conn in pipes:
            if conn.poll():
                tweets.extend(conn.recv())
                conn.close()
                pipes.remove(conn)
    retvals = [p.join() for p in processes]
    complete.set()
    rate_limiter.join()
    if pipe:
        pipe.send(tweets)
        pipe.close()
    return retvals


def show_status_info(session, token):
    status = json.loads(
        session.get('https://api.twitter.com/1.1/application/rate_limit_status.json?resources=search', auth=token).text)
    print(status)
    print(status['resources']['search']['/search/tweets']['remaining'])
    reset_time = status['resources']['search']['/search/tweets']['reset']
    print(time.strftime("%Z - %Y/%m/%d, %H:%M:%S", time.localtime(reset_time)))
    print('{} seconds until reset'.format(
        math.ceil((datetime.datetime.fromtimestamp(reset_time) - datetime.datetime.now()).total_seconds())))


def count_tweets():
    files = [
        'pickle_(expo_Action_Figure)_OR_(expo_Video_game)',
        'pickle_(expo_Lego)_OR_(expo_E3)',
        'pickle_(expo_PS4)_OR_(expo_Xbox)_OR_(expo_PC)',
        'pickle_(expo_Switch)_OR_(expo_Playstation)',
        'pickle_(expo_game)_OR_(expo_gaming)',
        'pickle_(launch_Action_Figure)',
        'pickle_(launch_E3)_OR_(launch_Switch)',
        'pickle_(launch_Playstation)_OR_(launch_PS4)',
        'pickle_(launch_Xbox)_OR_(launch_PC)',
        'pickle_(launch_gaming)_OR_(trending_gimmick)',
        'pickle_(launch_gimmick)_OR_(launch_Lego)',
        'pickle_(love_Action_Figure)_OR_(love_Video_game)',
        'pickle_(love_Lego)_OR_(love_E3)',
        'pickle_(preorder_Action_Figure)',
        'pickle_(preorder_Lego)_OR_(preorder_E3)',
        'pickle_(preorder_Playstation)_OR_(preorder_PS4)',
        'pickle_(preorder_Switch)',
        'pickle_(preorder_Video_game)_OR_(preorder_game)',
        'pickle_(preorder_Xbox)_OR_(preorder_PC)',
        'pickle_(preorder_gaming)_OR_(expo_gimmick)',
        'pickle_(release_Action_Figure)',
        'pickle_(release_Xbox)_OR_(release_PC)',
        'pickle_(release_gaming)_OR_(love_gimmick)',
        'pickle_(release_gimmick)_OR_(release_Lego)',
        'pickle_(trending_Action_Figure)',
        'pickle_(trending_Lego)_OR_(trending_E3)',
        'pickle_(trending_Playstation)_OR_(trending_PS4)',
        'pickle_(trending_Switch)',
        'pickle_(trending_Video_game)_OR_(trending_game)',
        'pickle_(trending_Xbox)_OR_(trending_PC)',
        'pickle_(trending_gaming)_OR_(preorder_gimmick)',
    ]
    files = ['/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickles' + file for file in files]
    tweets = []
    for file in files:
        with open(file, 'rb') as f:
            tweets.extend(pickle.load(f))
    print(len(tweets))


if __name__ == '__main__':
    terms = ['launch gimmick', 'launch Lego', 'launch E3', 'launch Switch', 'launch Playstation', 'launch PS4',
             'launch Xbox', 'launch PC', 'launch Action Figure', 'launch Video game', 'launch game',
             'launch gaming', 'trending gimmick', 'trending Lego', 'trending E3', 'trending Switch',
             'trending Playstation', 'trending PS4', 'trending Xbox', 'trending PC', 'trending Action Figure',
             'trending Video game', 'trending game', 'trending gaming', 'preorder gimmick', 'preorder Lego',
             'preorder E3', 'preorder Switch', 'preorder Playstation', 'preorder PS4', 'preorder Xbox',
             'preorder PC', 'preorder Action Figure', 'preorder Video game', 'preorder game', 'preorder gaming',
             'expo gimmick', 'expo Lego', 'expo E3', 'expo Switch', 'expo Playstation', 'expo PS4', 'expo Xbox',
             'expo PC', 'expo Action Figure', 'expo Video game', 'expo game', 'expo gaming', 'release gimmick',
             'release Lego', 'release E3', 'release Switch', 'release Playstation', 'release PS4',
             'release Xbox', 'release PC', 'release Action Figure', 'release Video game', 'release game',
             'release gaming', 'love gimmick', 'love Lego', 'love E3', 'love Switch', 'love Playstation',
             'love PS4', 'love Xbox', 'love PC', 'love Action Figure', 'love Video game', 'love game',
             'love gaming']
    logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S', format='%(asctime)s %(levelname)-8s %(message)s',
                        level='INFO')
    run_downloaders(terms, os.environ['TWITTER_CONSUMER_KEY'], os.environ['TWITTER_CONSUMER_SECRET'],
                    os.environ['TWITTER_ACCESS_TOKEN'], os.environ['TWITTER_ACCESS_SECRET'], category='test',
                    sleep_time=6)
