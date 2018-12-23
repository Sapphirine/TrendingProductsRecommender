import multiprocessing
import nltk.corpus
import argparse
import logging as logging
import os
import json
import warnings
import getpass
import findspark

from datetime import datetime
from multiprocessing import Process, Pipe
from nltk.corpus import wordnet as wn
from Memoized import Memoized

findspark.init()

import pyspark
from pyspark.sql import SQLContext, Window
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import col, lower, rand, row_number, lit
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, ChiSqSelector
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, CrossValidatorModel
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

LOGGER = logging.getLogger()

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    "--repositories http://repo.hortonworks.com/content/groups/public/ "
    "--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 "
    " pyspark-shell")

nltk.download('wordnet')

schema = StructType([
    StructField("marketplace", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("review_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_parent", StringType(), True),
    StructField("product_title", StringType(), True),  # "label" replaces "product_title"
    StructField("product_category", StringType(), True),
    StructField("star_rating", IntegerType(), True),
    StructField("helpful_votes", IntegerType(), True),
    StructField("total_votes", IntegerType(), True),
    StructField("vine", StringType(), True),
    StructField("verified_purchase", StringType(), True),
    StructField("review_headline", StringType(), True),
    StructField("review_body", StringType(), True),
    StructField("review_date", StringType(), True)])

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


class ProductAnalyzer(object):

    def __init__(self, sc: pyspark.SparkContext = None, input_file: str = None, model_prefix: str = None,
                 predictions_prefix: str = None, hdfs_base_url: str = None, category: str = None):
        self._model_prefix = model_prefix
        self._predictions_prefix = predictions_prefix
        self._hdfs_base_url = hdfs_base_url
        self.trigram_model = None
        self.input_tweets = None
        self._source_data = None
        self._preprocess_pipeline = None
        self.predictions = None
        self._fitted_lr_model = None
        self.table_name = None
        self._category = category

        self._sc = sc
        if self._sc:
            self._sqlc = SQLContext(self._sc)
        self._input_file = input_file
        if self._input_file:
            self.load_data()

    @property
    def source_data(self):
        return self._source_data

    def load_data(self):
        LOGGER.warning('Loading {}'.format(self._input_file))
        self._source_data = self._sqlc.read.format('com.databricks.spark.csv').options(header='true',
                                                                                       schema=schema,
                                                                                       delimiter="\t").load(
            self._input_file).withColumnRenamed('review_body', 'review_body_raw')
        self._source_data = self._source_data \
            .dropna() \
            .drop('marketplace', 'customer_id', 'review_id', 'product_id', 'product_parent', 'product_category',
                  'star_rating', 'helpful_votes', 'total_votes', 'vine', 'verified_purchase', 'review_headline',
                  'review_date') \
            .where('length(review_body_raw)>200') \
            .withColumn("review_body", lower(col("review_body_raw"))) \
            .drop("review_body_raw")
        LOGGER.warning('Loaded {} product reviews from {}.'.format(self._source_data.count(), self._input_file))

    def resample_data(self):
        LOGGER.warning('Resampling data.')
        counts = self._source_data.groupBy('product_title').count().selectExpr("product_title as product_title_tmp",
                                                                               "count as count")
        self._source_data = self._source_data \
            .join(counts, self._source_data.product_title == counts.product_title_tmp) \
            .drop('product_title_tmp') \
            .where("count > 1000")
        min_count = self._source_data.groupBy("product_title").count().agg({"count": "min"}).collect()[0][0]
        # Take a random sample from each product's entries, with a sample size
        # equal to the size of the smallest corpus.
        w = Window.partitionBy(col("product_title")).orderBy(col("rnd_"))

        self._source_data = (self._source_data
                             .withColumn("rnd_", rand())  # Add random numbers column
                             .withColumn("rn_", row_number().over(w))  # Add rowNumber over window
                             .where(col("rn_") <= min_count)  # Take n observations
                             .drop("rn_")  # drop helper columns
                             .drop("rnd_"))

        LOGGER.warning('Resampled down to {} examples per product'.format(min_count))

    @staticmethod
    @Memoized
    def get_stopwords():

        list_adv = []
        list_adj = []
        list_n = []

        for s in wn.all_synsets():
            if s.pos() in ['r']:  # if synset is adverb
                for i in s.lemmas():  # iterate through lemmas for each synset
                    list_adv.append(i.name())
            elif s.pos() in ['a']:
                for i in s.lemmas():  # iterate through lemmas for each synset
                    list_adj.append(i.name())
            elif s.pos() in ['n']:  # if synset is noun
                for i in s.lemmas():  # iterate through lemmas for each synset
                    list_n.append(i.name())
        # remove stop words and irrelevant words
        add_stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
                         "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it",
                         "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who",
                         "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been",
                         "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and",
                         "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about",
                         "against", "between", "into", "through", "during", "before", "after", "above", "below", "to",
                         "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then",
                         "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few",
                         "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so",
                         "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now", "ve", "se",
                         "didn", "hasn", "hadn", "hasnt", "isnt", "havent", "although", "despite", "however"]
        add_irrelevantwords = ["poor", "perfect", "good", "excellent", "excelent", "great", "horrible", "cheap",
                               "expensive", "different", "awesome"]
        single_alphabet = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
                           "s", "t", "u", "v", "w", "x", "y", "z"]

        # word_filter is a concatenated string of all unnecessary words
        word_filter = add_stopwords + add_irrelevantwords + single_alphabet + list_adv + list_adj
        return StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(word_filter)

    def build_preprocess_pipeline(self, vocab_size=10000, min_df=5):
        LOGGER.warning("Building preprocessing pipeline.")
        # regular expression tokenizer
        regex_tokenizer = RegexTokenizer(inputCol="review_body", outputCol="words", pattern="[^A-Za-z]+",
                                         toLowercase=True)
        stopwords = ProductAnalyzer.get_stopwords()
        count_vectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=vocab_size, minDF=min_df)
        label_string_idx = StringIndexer(inputCol="product_title", outputCol="label")
        self._preprocess_pipeline = Pipeline(stages=[regex_tokenizer, stopwords, count_vectors, label_string_idx])
        # self._preprocess_pipeline = Pipeline(stages=[regex_tokenizer, stopwords, count_vectors])
        LOGGER.warning("Built preprocessing pipeline.")
        return self._preprocess_pipeline

    def fit_preprocess_pipeline(self, save: bool = True):
        LOGGER.warning("Fitting preprocessing model.")
        if self._preprocess_pipeline:
            pipeline = self._preprocess_pipeline
        else:
            pipeline = self.build_preprocess_pipeline()

        fitted = pipeline.fit(self.source_data)

        if save:
            # ProductAnalyzer.save_model(fitted,
            #                            self._hdfs_base_url + 'models/' +
            #                            self._model_prefix + '_preprocess_pipeline.model')
            ProductAnalyzer.save_model(fitted,
                                       self._hdfs_base_url +
                                       'models/' +
                                       self._model_prefix +
                                       "_" +
                                       self._category +
                                       '_fit_preprocess_pipeline.model')

        self._source_data = fitted.transform(self.source_data).where('label < 10').drop('count', 'words', 'filtered')
        LOGGER.warning("Fitted preprocessing model.")
        return fitted

    @staticmethod
    def run_feature_selection_on(data):
        LOGGER.warning("Running feature selection.")
        selector = ChiSqSelector(numTopFeatures=10, featuresCol="features", outputCol="selectedFeatures",
                                 labelCol="label")

        data = selector.fit(data).transform(data).drop(
            'features').withColumnRenamed('selectedFeatures', 'features')
        LOGGER.warning("Ran feature selection.")
        return data

    def run_feature_selection(self):
        return self.run_feature_selection_on(self._source_data)

    def train(self, train_set, save: bool = True):
        LOGGER.warning("Training.")
        lrt = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0.8)

        param_grid = (ParamGridBuilder()
                      .addGrid(lrt.regParam, [0.1, 0.3, 0.5])  # regularization parameter
                      .addGrid(lrt.elasticNetParam, [0.0, 0.1, 0.2])  # Elastic Net Parameter (Ridge = 0)
                      .build())
        # 5-fold CrossValidator
        cv = CrossValidator(estimator=lrt, estimatorParamMaps=param_grid, evaluator=MulticlassClassificationEvaluator(),
                            numFolds=5)
        self._fitted_lr_model = cv.fit(train_set)
        if save:
            ProductAnalyzer.save_model(self._fitted_lr_model,
                                       self._hdfs_base_url +
                                       'models/' +
                                       self._model_prefix +
                                       "_" +
                                       self._category +
                                       '_fitted_lr.model')
        LOGGER.warning("Training complete.")
        return self._fitted_lr_model

    def load_model(self, fieldname, load_file, klass=PipelineModel):
        LOGGER.warning("Loading model from {} into {}".format(load_file, fieldname))
        setattr(self, fieldname, klass.load(load_file))
        return getattr(self, fieldname)

    @staticmethod
    def save_model(model, save_file):
        model.write().overwrite().save(save_file)

    def validate(self, val_set):
        LOGGER.warning("Validating.")
        evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
        predictions = self._fitted_lr_model.transform(val_set)
        evaluation = evaluator.evaluate(predictions)
        LOGGER.warning("Validation complete.")
        return evaluation

    def predict(self, data):
        LOGGER.warning("Predicting.")
        predictions = self._fitted_lr_model.transform(data)
        self.predictions = predictions
        return predictions

    def read_table(self, table_name: str):
        LOGGER.warning("Reading from input table {}.".format(table_name))
        catalog = CATALOG
        catalog['table']['name'] = table_name
        self.input_tweets = self._sqlc.read.options(catalog=json.dumps(catalog)).format(DATA_SOURCE_FORMAT).load()
        return self.input_tweets

    def save_predictions(self, predictions=None, table_name: str = None):
        table_name = table_name or self.table_name
        predictions = predictions or self.predictions
        if predictions is None:
            raise ValueError('No predictions provided or saved in the analyzer.')
        if table_name is None:
            raise ValueError('No hbase table_name provided or saved in the analyzer.')
        predictions.select('row_id', 'prediction').write.options(
            catalog=json.dumps(
                {
                    "table": {
                        "namespace": "default",
                        "name": table_name},
                    "rowkey": "row_id",
                    "columns": {
                        "row_id": {
                            "cf": "rowkey", "col": "row_id", "type": "string"
                        },
                        "prediction": {
                            "cf": "tweet", "col": "product", "type": "string"
                        }
                    }
                }),
            newtable=5).format(DATA_SOURCE_FORMAT).save()


def train_val_split(data):
    return data.randomSplit([0.85, 0.15], seed=100)


def run_analyzer(hbase_table, model_prefix, predictions_prefix, inputs, pipe, hdfs_base_url):
    try:
        sc = pyspark.SparkContext()
        sc.setLogLevel("ERROR")
    except ValueError as err:
        warnings.warn("SparkContext already exists in this scope")
        raise err
    # analyzer = ProductAnalyzer(sc, inputs[0], model_prefix, predictions_prefix, hdfs_base_url, inputs[2])
    analyzer = ProductAnalyzer(sc, inputs[0], model_prefix, predictions_prefix, hdfs_base_url, inputs[2])
    trained = analyzer.load_model('_fitted_lr_model', hdfs_base_url +
                                  'models/' +
                                  '20181222_072314_alexdziena_product_analyzer' +
                                  "_" +
                                  inputs[2] +
                                  '_fitted_lr.model', CrossValidatorModel)
    tweets = analyzer.read_table(hbase_table).where(col('category') == inputs[2])
    tweets = tweets.withColumnRenamed('text', 'review_body').withColumn('product_title',lit('unknown'))
    analyzer.resample_data()
    preprocessor = analyzer.build_preprocess_pipeline()
    analyzer.fit_preprocess_pipeline()
    processed_data = analyzer.run_feature_selection()
    # train, test = train_val_split(processed_data)
    # trained = analyzer.train(train)
    # LOGGER.warning('Accuracy: {}'.format(analyzer.validate(test)))

    preprocessed = preprocessor.fit(tweets).transform(tweets).drop('count', 'words', 'filtered')
    preprocessed = ProductAnalyzer.run_feature_selection_on(preprocessed)
    predictions = analyzer.predict(preprocessed)
    predictions.show()


    selected = processed_data.select("product_title", "label")
    distinct_pts = selected.alias('distinct_pts').selectExpr('product_title as predicted_pt',
                                                             'label as lookup_label').distinct()
    predicted_pts = predictions.alias('predicted_pts').select('prediction', 'row_id')
    joined = predicted_pts.join(distinct_pts, predicted_pts.prediction == distinct_pts.lookup_label, 'left')
    joined.orderBy(rand()).show(1000)
    analyzer.save_predictions(
        predictions=joined.drop('prediction').drop('lookup_label').withColumnRenamed('predicted_pt', 'prediction'),
        table_name=hbase_table)
    pipe.send(predictions)
    pipe.close()


def main(hbase_table, model_prefix, predictions_prefix, reviews_files, process_names, hdfs_base_url, categories):
    process_and_pipes = []
    for p in zip(reviews_files, process_names, categories):
        parent_conn, child_conn = Pipe()
        proc = Process(target=run_analyzer,
                       args=(hbase_table, model_prefix, predictions_prefix, p, child_conn, hdfs_base_url),
                       name=p[1])
        process_and_pipes.append(
            (proc, parent_conn)
        )
        proc.start()
    analyzers = []
    for p in process_and_pipes:
        pipe = p[1]
        proc = p[0]
        analyzers.append(pipe.recv())
        pipe.close()
        proc.join()
    print(analyzers)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Twitter streamer parameters.')
    parser.add_argument('-l', '--loglevel', help='One of: DEBUG, INFO, WARNING, ERROR, CRITICAL', type=str,
                        default='WARNING')
    parser.add_argument('--logfile', help='Filename to write to. If not specified, write logs to stderr', type=str,
                        default=None)
    parser.add_argument('--hbase_table', help='Name of HBase table to which tweets will be written', type=str,
                        default='20181210_224422_alexdziena_tweets')
    parser.add_argument('--model_prefix', help='Prefix for model filenames', type=str,
                        default='{}_{}_product_analyzer'.format(datetime.now().strftime("%Y%m%d_%H%M%S"),
                                                                getpass.getuser()))
    parser.add_argument('--predictions_prefix', help='Prefix for predictions filenames', type=str,
                        default='{}_{}_product_analyzer'.format(datetime.now().strftime("%Y%m%d_%H%M%S"),
                                                                getpass.getuser()))
    parser.add_argument('--hdfs_base_url', help='Base url for interacting with hdfs', type=str,
                        default='hdfs://big-data-analytics:1234/')
    parser.add_argument('-f', '--reviews_files',
                        help='space delimited list of file paths of product reviews, minimum of one path', nargs='+',
                        type=str)
    parser.add_argument('-n', '--process_names',
                        help=('space delimited list of file of process names, used for model saving filepaths, '
                              'one per review file'),
                        nargs='+',
                        type=str)
    parser.add_argument('-c', '--categories',
                        help=('space delimited list of categories to map to review files, '
                              'one per review file'),
                        nargs='+',
                        type=str)
    args = parser.parse_args()
    loglevel = args.loglevel.upper()
    if args.logfile is None:
        logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S', format='%(asctime)s %(levelname)-8s %(message)s',
                            level=loglevel)
    else:
        logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S', format='%(asctime)s %(levelname)-8s %(message)s',
                            filename=args.logfile, level=loglevel)
    LOGGER = multiprocessing.log_to_stderr()
    LOGGER.info(args)
    # hbase_table_prefix = None if str.lower(args.hbase_table_prefix) == 'none' else args.hbase_table_prefix
    main(args.hbase_table, args.model_prefix, args.predictions_prefix, args.reviews_files, args.process_names,
         args.hdfs_base_url, args.categories)