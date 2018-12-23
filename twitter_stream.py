import findspark
import sys
import requests
import requests_oauthlib
import json
import socket
import os
import argparse
import logging
import jstyleson
import getpass
import itertools

findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from multiprocessing import Process, Pipe
from datetime import datetime
from urllib.parse import urlencode
from Memoized import Memoized
from HBase import HBaseClient
from Worker import run_downloaders
from util.util import get_phrases, chunks


@Memoized
def get_oauth(consumer_key: str, consumer_secret: str, access_token: str, access_secret: str):
    return requests_oauthlib.OAuth1(consumer_key, consumer_secret, access_token, access_secret)


def get_tweets(session, query_terms, oauth_token):
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_params = urlencode({'language': 'en', 'track': ','.join(query_terms)}).replace('+', '%20')
    query_url = url + '?' + query_params
    response = session.get(query_url, auth=oauth_token,
                           stream=True)
    logging.warning("%s: %s", query_url, response)
    return response


def send_tweets_to_spark(http_resp, tcp_connection, client: HBaseClient, table, category):
    for line in http_resp.iter_lines():
        if len(line) > 0:
            try:
                full_tweet = json.loads(line.decode('utf-8'))
                full_tweet['category'] = category

                # Filter these out of streamed tweets, they are always 0 and might overwrite real values from searched tweets
                for key in ['favorite_count', 'retweet_count']:
                    if key in full_tweet:
                        del full_tweet[key]

                client.write_tweet_to_hbase(full_tweet, table)
                # print(full_tweet)
                tweet_text = full_tweet['text']
                # print(tweet_text)
                tcp_connection.send((tweet_text + '\n').encode())
            except Exception as exc:
                logging.warning(exc)
                e = sys.exc_info()[0]
                print(e)


def process_rdd(time, rdd):
    if not rdd.isEmpty():
        logging.info("----------- %s -----------" % str(time))
        hashtag_counts_df = None
        try:
            # Get spark sql singleton context from the current context
            sql_context = get_sql_context_instance(rdd.context)
            # convert the RDD to Row RDD
            row_rdd = rdd.map(lambda w: Row(term=w[0], term_count=w[1]))
            # create a DF from the Row RDD
            if not row_rdd.isEmpty():
                hashtags_df = sql_context.createDataFrame(row_rdd)
                # Register the dataframe as table
                hashtags_df.registerTempTable("search_terms")
                # get the top 10 hashtags from the table using SQL and print them
                all_terms = []
                for v in query_terms.values():
                    all_terms += v
                hashtag_counts_df = sql_context.sql(
                    "select `term`, `term_count` from `search_terms` WHERE `term` IN {0}".format(
                        tuple(all_terms)
                    )
                )
        except Exception as e:
            print(e)
        finally:
            try:
                pass
                # os.system('cls' if os.name == 'nt' else 'clear')
                # if hashtag_counts_df is not None:
                #     hashtag_counts_df \
                #         .orderBy('term_count', ascending=False) \
                #         .show(10)
            except Exception as e:
                print(e)


def sum_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def stream_to_spark(name, host, port):
    conf = SparkConf()
    conf.setAppName(name)
    # create spark context with the above configuration
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    # create the Streaming Context from the above spark context with interval
    # size 1 seconds
    ssc = StreamingContext(sc, 1)
    # setting a checkpoint to allow RDD recovery
    ssc.checkpoint("checkpoint_TwitterApp")
    # read data from port 9009
    data_stream = ssc.socketTextStream(host, port)
    # dataStream.pprint()
    # split each tweet into words
    words = data_stream.flatMap(lambda line: line.split(" "))
    # filter the words to get only hashtags, then map each hashtag to
    # # be a pair of (hashtag,1)
    # words = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
    words = words.map(lambda x: (x, 1))
    # # adding the count of each hashtag to its last count
    # tags_totals = hashtags.updateStateByKey(sum_tags_count)
    tags_totals = words.updateStateByKey(sum_tags_count)
    # do processing for each RDD generated in each interval
    tags_totals.foreachRDD(process_rdd)
    # start the streaming computation
    ssc.start()
    # wait for the streaming to finish
    ssc.awaitTermination()





def main(
        query_terms,
        consumer_key,
        consumer_secret,
        access_token,
        access_secret,
        spark_host,
        spark_port,
        spark_app_name,
        num_ports,
        hbase_table,
        hbase_connection_pool_size,
        hbase_host,
        hbase_port,
        hbase_thrift_version,
        hbase_table_prefix
):
    logging.warning("Using table {}_{}".format(hbase_table_prefix, hbase_table))
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    target_port = spark_port
    for target_port in range(spark_port, spark_port + num_ports):
        try:
            s.bind((spark_host, target_port))
            logging.warning('Bound socket to %s:%d, listening', spark_host, target_port)
            break
        except OSError:
            logging.warning('Port %d unavailable, trying next port', target_port)
            if target_port == spark_port + num_ports - 1:
                logging.error('No ports available! Exiting.')
                exit(1)
    s.listen(1)
    print("Waiting for TCP connection...")
    hbase_client = HBaseClient(
        hbase_connection_pool_size,
        host=hbase_host,
        port=hbase_port,
        compat=hbase_thrift_version,
        table_prefix=hbase_table_prefix)
    hbase_client.create_table_if_not_exists(hbase_table)

    spark_process = Process(target=stream_to_spark, args=(spark_app_name, spark_host, target_port))
    spark_process.start()
    conn, addr = s.accept()
    print("Connected... Starting getting tweets.")
    processes = []
    session = requests.session()
    oauth_token = get_oauth(consumer_key, consumer_secret, access_token, access_secret)
    phrases = get_phrases(query_terms)
    downloaders = []
    downloaders_pipes = []
    # We get 180 requests per 15 min, so we need to have the worker threads sleep for sleep_time between requests
    sleep_time = 15*60/(180/len(phrases))
    for category in phrases:
        if phrases[category]:
            # Streamers
            for chunk in chunks(phrases[category], 1000):
                resp = get_tweets(session, chunk, oauth_token)
                p = Process(target=send_tweets_to_spark, args=(resp, conn, hbase_client, hbase_table, category))
                processes.append(p)
                p.start()

            #Downloaders
            parent_conn, child_conn = Pipe()
            downloader = Process(target=run_downloaders,
                                  args=(phrases[category], consumer_key, consumer_secret, access_token, access_secret, child_conn, category, hbase_client, hbase_table, sleep_time))
            downloaders.append(downloader)
            downloaders_pipes.append(parent_conn)
            downloader.start()
    while downloaders_pipes:
        for conn in downloaders_pipes:
            if conn.poll():
                tweets = conn.recv()
                logging.warning('Loaded {} tweets for category {} from search endpoint'.format(len(tweets),tweets[0]['category']))
                conn.close()
                downloaders_pipes.remove(conn)

    print([p.join() for p in downloaders])
    print([p.join() for p in processes])
    spark_process.join()
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Twitter streamer parameters.')
    parser.add_argument('-f', '--hashtags_file', help='A JSON file of hashtags category: [hashtags] pairs.', type=open,
                        default='terms.json')
    parser.add_argument('-l', '--loglevel', help='One of: DEBUG, INFO, WARNING, ERROR, CRITICAL', type=str,
                        default='WARNING')
    parser.add_argument('--logfile', help='Filename to write to. If not specified, write logs to stderr', type=str,
                        default=None)
    parser.add_argument('--spark_host', help='Host for spark server', type=str,
                        default='localhost')
    parser.add_argument('--spark_port', help='Port for spark server', type=int,
                        default=9009)
    parser.add_argument('--spark_app_name', help='Spark AppName for spark server. ', type=str,
                        default='TwitterStreamApp')
    parser.add_argument('--num_ports', help='Number of ports to try before giving up', type=int,
                        default=100)
    parser.add_argument('--hbase_table', help='Name of HBase table to which tweets will be written', type=str,
                        default='tweets')
    parser.add_argument('--hbase_connection_pool_size', help='HBase connection pool size', type=int,
                        default=10)
    parser.add_argument('--hbase_host', help='hbase host', type=str,
                        default='localhost')
    parser.add_argument('--hbase_port', help='hbase port', type=int,
                        default=9090)
    parser.add_argument('--hbase_thrift_version', help='hbase thrift version', type=str,
                        default='0.92')
    parser.add_argument('--hbase_table_prefix', help='hbase table prefix / namespace', type=str,
                        default='{}_{}'.format(datetime.now().strftime("%Y%m%d_%H%M%S"), getpass.getuser()))
    args = parser.parse_args()
    loglevel = args.loglevel.upper()
    if args.logfile is None:
        logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S', format='%(asctime)s %(levelname)-8s %(message)s',
                            level=loglevel)
    else:
        logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S', format='%(asctime)s %(levelname)-8s %(message)s',
                            filename=args.logfile, level=loglevel)
    with args.hashtags_file as hf:
        query_terms = jstyleson.load(hf)
    logging.info(args)
    logging.info(query_terms)
    # Twitter credentials are read from env vars
    consumer_key, consumer_secret, access_token, access_secret = os.environ['TWITTER_CONSUMER_KEY'], \
                                                                 os.environ['TWITTER_CONSUMER_SECRET'], \
                                                                 os.environ['TWITTER_ACCESS_TOKEN'], \
                                                                 os.environ['TWITTER_ACCESS_SECRET']
    spark_host, spark_port, spark_app_name = args.spark_host, args.spark_port, args.spark_app_name
    hbase_table_prefix = None if str.lower(args.hbase_table_prefix) == 'none' else args.hbase_table_prefix
    main(
        query_terms,
        consumer_key,
        consumer_secret,
        access_token,
        access_secret,
        spark_host,
        spark_port,
        spark_app_name,
        args.num_ports,
        args.hbase_table,
        args.hbase_connection_pool_size,
        args.hbase_host,
        args.hbase_port,
        args.hbase_thrift_version,
        hbase_table_prefix
    )
