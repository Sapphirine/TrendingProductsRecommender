import findspark
findspark.init()

import argparse
# import datetime
# import getpass
import logging
import os
import json
import pyspark
import warnings
from datetime import datetime
import getpass
from pyspark.sql import SQLContext
from pyspark.ml.feature import NGram, VectorAssembler
from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.feature import IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# pyspark.SparkContext.setSystemProperty('spark.executor.memory', '8g')
# pyspark.SparkContext.setSystemProperty('spark.driver.memory', '6g')

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    "--repositories http://repo.hortonworks.com/content/groups/public/ "
    "--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 "
    # "--executor-memory 5G "
    # "--driver-memory 2G "
    # "--num-executors 4 "
    # "--executor-cores 2 "
    " pyspark-shell")

DATA_SOURCE_FORMAT = 'org.apache.spark.sql.execution.datasources.hbase'
CATALOG = {
    "table": {"namespace": "default", "name": "tweets"},
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
    }
}
# tokenizer = Tokenizer(inputCol="text", outputCol="words")
# cv = CountVectorizer(vocabSize=2**16, inputCol="words", outputCol='cv')
# idf = IDF(inputCol='cv', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
# label_stringIdx = StringIndexer(inputCol = "target", outputCol = "label")
# lr = LogisticRegression(maxIter=100)
# pipeline = Pipeline(stages=[tokenizer, cv, idf, label_stringIdx, lr])
# 
# pipelineFit = pipeline.fit(train_set)
# predictions = pipelineFit.transform(val_set)
# accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(val_set.count())
# roc_auc = evaluator.evaluate(predictions)
# 
# print("Accuracy Score: {0:.4f}".format(accuracy))
# print("ROC-AUC: {0:.4f}".format(roc_auc))


class SentimentAnalyzer(object):

    def __init__(self, sc: pyspark.SparkContext = None, cleaned_tweets_file: str = None):
        self._sc = sc
        if self._sc:
            self._sqlc = SQLContext(self._sc)
        self._cleaned_tweets_file = cleaned_tweets_file
        if self._cleaned_tweets_file:
            self.load_cleaned_tweets()

        self.trigram_model = None
        self.input_tweets = None

    @property
    def source_data(self):
        return self._source_data

    def load_cleaned_tweets(self):
        logging.warning('Loading {}'.format(self._cleaned_tweets_file))
        self._source_data = self._sqlc.read.format('com.databricks.spark.csv').options(header='true',
                                                                                           inferschema='true').load(
                self._cleaned_tweets_file)
        self._source_data = self._source_data.dropna()
        logging.warning('Loaded {} cleaned tweets.'.format(self._source_data.count()))

    @staticmethod
    def build_trigrams(input_cols=("text", "target"), n=3):
        logging.warning("Building trigram model.")
        tokenizer = [Tokenizer(inputCol=input_cols[0], outputCol="words")]
        ngrams = [
            NGram(n=i, inputCol="words", outputCol="{0}_grams".format(i))
            for i in range(1, n + 1)
        ]

        cv = [
            CountVectorizer(vocabSize=2 ** 14, inputCol="{0}_grams".format(i),
                            outputCol="{0}_tf".format(i))
            for i in range(1, n + 1)
        ]
        idf = [IDF(inputCol="{0}_tf".format(i), outputCol="{0}_tfidf".format(i), minDocFreq=5) for i in
               range(1, n + 1)]

        assembler = [VectorAssembler(
            inputCols=["{0}_tfidf".format(i) for i in range(1, n + 1)],
            outputCol="rawFeatures"
        )]
        label_string_idx = [StringIndexer(inputCol=input_cols[1], outputCol="label")]
        selector = [ChiSqSelector(numTopFeatures=2 ** 14, featuresCol='rawFeatures', outputCol="features")]
        lr = [LogisticRegression(maxIter=100)]
        return Pipeline(stages=tokenizer + ngrams + cv + idf + assembler + label_string_idx + selector + lr)

    def train(self, train_set):
        logging.warning("Training.")
        self.trigram_model = self.build_trigrams().fit(train_set)
        logging.warning("Training complete.")
        return self.trigram_model

    def load_model(self, load_file):

        logging.warning("Loading model from {}".format(load_file))
        self.trigram_model = PipelineModel.load(load_file)

    def save_model(self, save_file):
        self.trigram_model.overwrite().save(save_file)

    def validate(self, val_set):
        logging.warning("Validating.")
        evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
        predictions = self.trigram_model.transform(val_set)
        accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(predictions.count())
        roc_auc = evaluator.evaluate(predictions)
        logging.warning('Accuracy: {}'.format(accuracy))
        logging.warning('ROC-AUC: {}'.format(roc_auc))
        logging.warning("Validation complete.")
        return predictions
    
    def predict(self, data):
        logging.warning("Predicting.")
        predictions = self.trigram_model.transform(data)
        self.predictions = predictions
        return predictions

    def read_tweets(self, table_name: str):
        logging.warning("Reading from input table {}.".format(table_name))
        catalog = CATALOG
        catalog['table']['name'] = table_name
        self.input_tweets = self._sqlc.read.options(catalog=json.dumps(catalog)).format(DATA_SOURCE_FORMAT).load()
        return self.input_tweets

    def save_predictions(self, predictions = None, table_name: str = None):
        table_name = table_name or self.table_name
        predictions = predictions or self.predictions
        if predictions is None:
            raise ValueError('No predictions provided or saved in the analyzer.')
        if table_name is None:
            raise ValueError('No hbase table_name provided or saved in the analyzer.')
        predictions.select('row_id', 'prediction').write.options(
            catalog=json.dumps(
                {
                    "table":{
                        "namespace": "default",
                        "name": table_name},
                    "rowkey": "row_id",
                    "columns": {
                        "row_id": {
                            "cf": "rowkey", "col": "row_id", "type": "string"
                        },
                        "prediction": {
                            "cf": "tweet", "col": "sentiment", "type": "boolean"
                        }
                    }
                }),
            newtable=5).format(DATA_SOURCE_FORMAT).save()



def main(cleaned_tweets_file, table_name, model_save_file, predictions_save_file):
    try:
        sc = pyspark.SparkContext()
    except ValueError as err:
        warnings.warn("SparkContext already exists in this scope")
        raise err
    # analyzer = SentimentAnalyzer(sc, cleaned_tweets_file)
    analyzer = SentimentAnalyzer(sc, 'clean_tweet.csv')
    (train_set, val_set) = analyzer.source_data.randomSplit([0.90, 0.10], seed=2000)
    # (train_set, val_set) = analyzer.source_data.randomSplit([0.001, 0.001], seed=2000)
    # analyzer.train(train_set)
    # analyzer.save_model(model_save_file)
    analyzer.load_model('20181212_230600_alexdziena_trigram.pipeline.model')
    analyzer.validate(val_set)
    # Reading
    df = analyzer.read_tweets(table_name)

    input_tweets = df.selectExpr(
        "row_id",
        "text",
    )
    input_tweets.show(10)
    predictions = analyzer.predict(input_tweets)
    predictions.show()
    predictions.write.parquet(predictions_save_file)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Twitter streamer parameters.')
    parser.add_argument('-f', '--cleaned_tweets_file', help='Local filename or HDFS url to file of cleaned tweets.',
                        type=str, default='cleaned_tweets.csv')
    parser.add_argument('-l', '--loglevel', help='One of: DEBUG, INFO, WARNING, ERROR, CRITICAL', type=str,
                        default='WARNING')
    parser.add_argument('--logfile', help='Filename to write to. If not specified, write logs to stderr', type=str,
                        default=None)
    # parser.add_argument('--spark_app_name', help='Spark AppName for spark server. ', type=str,
    #                     default='SentimentAnalysisApp')
    parser.add_argument('--hbase_table', help='Name of HBase table to which tweets will be written', type=str,
                        default='tweets')
    parser.add_argument('--model_save_file', help='File path to which model should be saved', type=str,
                        default='{}_{}_trigram.pipeline.model'.format(datetime.now().strftime("%Y%m%d_%H%M%S"),
                                                                      getpass.getuser()))
    parser.add_argument('--predictions_save_file', help='File path to which model should be saved', type=str,
                        default='{}_{}_sentiment.predictions'.format(datetime.now().strftime("%Y%m%d_%H%M%S"),
                                                                      getpass.getuser()))
    # parser.add_argument('--hbase_host', help='hbase host', type=str,
    #                     default='localhost')
    # parser.add_argument('--hbase_port', help='hbase port', type=int,
    #                     default=1234)
    # parser.add_argument('--hbase_thrift_port', help='hbase port', type=int,
    #                     default=9090)
    # parser.add_argument('--hbase_thrift_version', help='hbase thrift version', type=str,
    #                     default='0.92')
    # parser.add_argument('--hbase_table_prefix', help='hbase table prefix / namespace', type=str,
    #                     default='{}_{}'.format(datetime.now().strftime("%Y%m%d_%H%M%S"), getpass.getuser()))
    args = parser.parse_args()
    loglevel = args.loglevel.upper()
    if args.logfile is None:
        logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S', format='%(asctime)s %(levelname)-8s %(message)s',
                            level=loglevel)
    else:
        logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S', format='%(asctime)s %(levelname)-8s %(message)s',
                            filename=args.logfile, level=loglevel)
    logging.info(args)
    # hbase_table_prefix = None if str.lower(args.hbase_table_prefix) == 'none' else args.hbase_table_prefix
    main(args.cleaned_tweets_file, args.hbase_table, args.model_save_file, args.predictions_save_file)
