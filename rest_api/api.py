import findspark

findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext
import os

from flask import Flask
from flask_restful import Resource, Api, reqparse
import argparse
from util.util import get_phrases
from rest_api.resources import GetTweets
from rest_api.resources import WordCount
from rest_api.resources import Stats
from datetime import datetime
import getpass
from HBase import HBaseClient
import json
import jstyleson
from flask_cors import CORS

app = Flask(__name__)
api = Api(app)
CORS(app)

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    "--repositories http://repo.hortonworks.com/content/groups/public/ "
    "--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 "
    # "--files \"/Users/alexdziena/projects/columbia/big-data-analytics/final_project/hbase-site.xml\" "
    # "--executor-memory 5G "
    # "--driver-memory 2G "
    # "--num-executors 4 "
    # "--executor-cores 2 "
    " pyspark-shell")

DATA_SOURCE_FORMAT = 'org.apache.spark.sql.execution.datasources.hbase'
TABLE = "20181210_224422_alexdziena_tweets"
CATALOG = catalog = json.dumps({
    "table": {"namespace": "default", "name": TABLE},
    "rowkey": "row_id",
    "columns": {
        "row_id": {"cf": "rowkey", "col": "row_id", "type": "string"},
        "category": {"cf": "tweet", "col": "category", "type": "string"},
        "contributors": {"cf": "tweet", "col": "contributors", "type": "string"},
        "coordinates": {"cf": "tweet", "col": "coordinates", "type": "string"},
        "created_at": {"cf": "tweet", "col": "created_at", "type": "string"},
        "display_text_range": {"cf": "tweet", "col": "display_text_range", "type": "string"},
        "entities": {"cf": "tweet", "col": "entities", "type": "string"},
        "extended_tweet": {"cf": "tweet", "col": "extended_tweet", "type": "string"},
        "favorite_count": {"cf": "tweet", "col": "favorite_count", "type": "bigint"},
        "favorited": {"cf": "tweet", "col": "favorited", "type": "boolean"},
        "filter_level": {"cf": "tweet", "col": "filter_level", "type": "string"},
        "geo": {"cf": "tweet", "col": "geo", "type": "string"},
        "id": {"cf": "tweet", "col": "id", "type": "bigint"},
        "in_reply_to_screen_name": {"cf": "tweet", "col": "in_reply_to_screen_name", "type": "string"},
        "in_reply_to_status_id": {"cf": "tweet", "col": "in_reply_to_status_id", "type": "string"},
        "in_reply_to_status_id_str": {"cf": "tweet", "col": "in_reply_to_status_id_str", "type": "string"},
        "in_reply_to_user_id": {"cf": "tweet", "col": "in_reply_to_user_id", "type": "string"},
        "in_reply_to_user_id_str": {"cf": "tweet", "col": "in_reply_to_user_id_str", "type": "string"},
        "is_quote_status": {"cf": "tweet", "col": "is_quote_status", "type": "boolean"},
        "lang": {"cf": "tweet", "col": "lang", "type": "string"},
        "place": {"cf": "tweet", "col": "place", "type": "string"},
        "quote_count": {"cf": "tweet", "col": "quote_count", "type": "bigint"},
        "reply_count": {"cf": "tweet", "col": "reply_count", "type": "bigint"},
        "retweet_count": {"cf": "tweet", "col": "retweet_count", "type": "bigint"},
        "retweeted": {"cf": "tweet", "col": "retweeted", "type": "boolean"},
        "source": {"cf": "tweet", "col": "source", "type": "string"},
        "text": {"cf": "tweet", "col": "text", "type": "string"},
        "timestamp_ms": {"cf": "tweet", "col": "timestamp_ms", "type": "bigint"},
        "truncated": {"cf": "tweet", "col": "truncated", "type": "boolean"},
        "sentiment": {"cf": "tweet", "col": "sentiment", "type": "boolean"},
        "product": {"cf": "tweet", "col": "product", "type": "string"},
    }
})


class HelloWorld(Resource):
    def get(self):
        return {'hello': 'world'}


def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='API parameters.')
    parser.add_argument('-f', '--terms_file', help='A JSON file of category: [terms] pairs.', type=open,
                        default='terms.json')
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
    parser.add_argument('--use_search_phrases',
                        help=('If true, use the full search phrases, e.g. "launch xbox" and "launch doritos", instead '
                              'of individual category search terms, e.g. "xbox" and "doritos"'),
                        type=bool,
                        default=False)
    args = parser.parse_args()
    hbase_client = HBaseClient(args.hbase_connection_pool_size, host=args.hbase_host, port=args.hbase_port,
                               compat=args.hbase_thrift_version, table_prefix=args.hbase_table_prefix)
    spark_conf = SparkConf().setAppName("rest-api").setMaster("local[4]")
    # spark_conf.set("spark.hbase.host", "big-data-analytics")
    # spark_conf.set("spark.hbase.port", "16000")
    # spark_conf.set("hbase.zookeeper.quorum", "35.211.101.139")
    # spark_conf.set("hbase.zookeeper.property.clientPort", "2181")
    sc = SparkContext(conf=spark_conf)
    sqlc = SQLContext(sc)
    with args.terms_file as hf:
        query_terms = jstyleson.load(hf)
    common_kwargs = {
                         'spark_context': sc,
                         'sql_context': sqlc,
                         'catalog': CATALOG,
                         'data_source_format': DATA_SOURCE_FORMAT,
                         'table': TABLE,
                     }
    api.add_resource(HelloWorld, '/')
    api.add_resource(GetTweets.GetTweets,
                     '/get_tweets',
                     '/get_tweets/<int:count>',
                     '/get_tweets/<int:count>/<string:sort>',
                     '/get_tweets/<int:count>/<string:sort>/<string:order>',
                     '/get_tweets/<int:count>/<string:sort>/<string:order>/<string:category>',
                     '/get_tweets/<int:count>/<string:sort>/<string:order>/<string:category>/<string:cache_clear>',
                     resource_class_kwargs=common_kwargs)
    api.add_resource(WordCount.WordCount,
                     '/wordcount/<string:by_type>',
                     '/wordcount/<string:by_type>/<string:where_filter>',
                     '/wordcount/<string:by_type>/<string:where_filter>/<string:cache_clear>',
                     resource_class_kwargs=merge_two_dicts(common_kwargs, {
                         'terms': get_phrases(query_terms) if args.use_search_phrases else query_terms
                     }))
    api.add_resource(Stats.Summary,
                     '/stats/summary',
                     '/stats/summary/<string:cache_clear>',
                     resource_class_kwargs=merge_two_dicts(common_kwargs, {
                         'terms': get_phrases(query_terms)
                     }))
    api.add_resource(Stats.Count,
                     '/stats/count/<string:by_type>',
                     '/stats/count/<string:where_filter>',
                     '/stats/count/<string:where_filter>/<string:cache_clear>',
                     resource_class_kwargs=common_kwargs)
    data_source_format = 'org.apache.spark.sql.execution.datasources.hbase'
    TABLE = "20181210_224422_alexdziena_tweets"

    # app.run(debug=True)
    app.run(host='0.0.0.0', port=8081)
