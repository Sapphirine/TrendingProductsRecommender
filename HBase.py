import happybase
import struct
import argparse
import logging
from datetime import datetime
import getpass
import pickle

HBASE_SCHEMA = {
    'tweet': dict(),
    'user': dict()
}


class HBaseClient(happybase.ConnectionPool):

    @staticmethod
    def prepare_tweet(tweet: dict):
        user = {}
        if 'user' in tweet:
            for k, v in tweet['user'].items():
                if isinstance(v, int):
                    user['user:{}'.format(k).encode('utf-8')] = struct.pack(">Q", v)
                else:
                    user['user:{}'.format(k).encode('utf-8')] = str(v).encode('utf-8')
        rowkey = tweet['id_str'].encode('utf-8')
        for key in ['user', 'entities', 'extended_entities', 'id_str', 'metadata']:
            if key in tweet:
                del tweet[key]
        enctweet = {}
        for k, v in tweet.items():
            if isinstance(v, int):
                enctweet['tweet:{}'.format(k).encode('utf-8')] = struct.pack(">Q", v)
            else:
                enctweet['tweet:{}'.format(k).encode('utf-8')] = str(v).encode('utf-8')
        return rowkey, enctweet, user

    def write_tweets_to_hbase(self, tweets: list, table: str):
        with self.connection() as connection:
            connection.open()
            table = connection.table(table)
            with table.batch(batch_size=1000) as b:
                for tweet in tweets:
                    tweet = HBaseClient.prepare_tweet(tweet)
                    rowkey = tweet[0]
                    enctweet = tweet[1]
                    user = tweet[2]
                    b.put(
                        rowkey,
                        {**user, **enctweet}
                    )
            connection.close()

    def write_tweet_to_hbase(self, tweet: dict, table: str):
        tweet = HBaseClient.prepare_tweet(tweet)
        rowkey = tweet[0]
        enctweet = tweet[1]
        user = tweet[2]
        with self.connection() as connection:
            connection.open()
            connection.table(table).put(
                rowkey,
                {**user, **enctweet}
            )
            connection.close()

    def drop_table(self, table: str):
        with self.connection() as connection:
            connection.open()
            connection.delete_table(table, disable=True)
            connection.close()

    def create_table_if_not_exists(self, table: str, schema: dict = None):
        with self.connection() as connection:
            connection.open()
            if str.encode(table) not in connection.tables():
                connection.create_table(table, schema or HBASE_SCHEMA)
                logging.warning('Created table {}_{}'.format(connection.table_prefix.decode("utf-8"), table))
            connection.close()


def main(table, connection_pool_size, host, port, thrift_version, table_prefix):
    client = HBaseClient(
        connection_pool_size,
        host=host,
        port=port,
        compat=thrift_version,
        table_prefix=table_prefix)
    files = [
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(expo_Action_Figure)_OR_(expo_Video_game)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(expo_Lego)_OR_(expo_E3)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(expo_PS4)_OR_(expo_Xbox)_OR_(expo_PC)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(expo_Switch)_OR_(expo_Playstation)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(expo_game)_OR_(expo_gaming)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(launch_Action_Figure)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(launch_E3)_OR_(launch_Switch)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(launch_Playstation)_OR_(launch_PS4)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(launch_Xbox)_OR_(launch_PC)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(launch_gaming)_OR_(trending_gimmick)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(launch_gimmick)_OR_(launch_Lego)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(love_Action_Figure)_OR_(love_Video_game)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(love_Lego)_OR_(love_E3)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(preorder_Action_Figure)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(preorder_Lego)_OR_(preorder_E3)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(preorder_Playstation)_OR_(preorder_PS4)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(preorder_Switch)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(preorder_Video_game)_OR_(preorder_game)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(preorder_Xbox)_OR_(preorder_PC)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(preorder_gaming)_OR_(expo_gimmick)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(release_Action_Figure)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(release_Xbox)_OR_(release_PC)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(release_gaming)_OR_(love_gimmick)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(release_gimmick)_OR_(release_Lego)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(trending_Action_Figure)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(trending_Lego)_OR_(trending_E3)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(trending_Playstation)_OR_(trending_PS4)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(trending_Switch)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(trending_Video_game)_OR_(trending_game)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(trending_Xbox)_OR_(trending_PC)',
        '/Users/alexdziena/projects/columbia/big-data-analytics/final_project/pickle_(trending_gaming)_OR_(preorder_gimmick)',
    ]
    tweets = []
    for file in files:
        with open(file, 'rb') as f:
            tweets.extend(pickle.load(f))
    client.create_table_if_not_exists(table)
    client.write_tweets_to_hbase(tweets, table)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='HBase Parameters.')
    parser.add_argument('-l', '--loglevel', help='One of: DEBUG, INFO, WARNING, ERROR, CRITICAL', type=str,
                        default='WARNING')
    parser.add_argument('--logfile', help='Filename to write to. If not specified, write logs to stderr', type=str,
                        default=None)
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
    hbase_table_prefix = None if str.lower(args.hbase_table_prefix) == 'none' else args.hbase_table_prefix
    main(
        args.hbase_table,
        args.hbase_connection_pool_size,
        args.hbase_host,
        args.hbase_port,
        args.hbase_thrift_version,
        hbase_table_prefix
    )
