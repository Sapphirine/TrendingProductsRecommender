from flask_restful import abort
from Memoized import Memoized
from rest_api.resources.BaseResource import BaseResource
import json
import pyspark.sql.functions as sql_funcs
import re


DEFAULT_WHERE_CLAUSE = ('sentiment = True and (favorite_count > 0 or retweet_count > 0 '
                        'or quote_count > 0 or reply_count > 0)')


class WordCount(BaseResource):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.terms = kwargs['terms']

    def _get_by_keywords(self, where_filter: str):
        prog = re.compile(r'"(.*)"')
        counts = []
        query = self.df
        for category in self.terms:
            if category != 'general':
                cat_terms = self.terms[category]
                if cat_terms:
                    for term in cat_terms:
                        match = prog.match(term)
                        if match:
                            print(match.groups())
                            like_expr = '%' + '%'.join(match.groups()).lower() + '%'
                        else:
                            like_expr = '%' + term.lower().replace('%', '\%').replace(' ', '%').replace('"', '') + '%'
                        col_name = term.lower().replace('%', '\%').replace(' ', '_').replace('"', '')
                        query = query.withColumn(col_name, sql_funcs.lower(sql_funcs.col('text')).like(like_expr))
                        counts.append(
                            sql_funcs.sum(sql_funcs.when(sql_funcs.col(col_name), 1)).alias(col_name)
                        )
        if where_filter == 'none':
            return json.loads(query.select(counts).toJSON().collect()[0])
        else:
            return json.loads(query.filter(where_filter).select(counts).toJSON().collect()[0])

    def _get_by_products(self, where_filter: str):
        if where_filter == 'none':
            counts = self.df.groupBy('product').count().toJSON().collect()
        else:
            counts = self.df.filter(where_filter).groupBy('product').count().toJSON().collect()
        ret = {}
        for count in counts:
            j = json.loads(count)
            ret[j['product']] = j['count']
        return ret

    @Memoized
    def get(self, by_type, where_filter: str = DEFAULT_WHERE_CLAUSE, cache_clear=None):
        if by_type not in ['keywords', 'products']:
            abort(404, message="wordcount only supports 'keywords' and 'products', got {}".format(by_type))
        if where_filter == 'default':
            where_filter = DEFAULT_WHERE_CLAUSE
        if by_type == 'keywords':
            return self._get_by_keywords(where_filter)
        if by_type == 'products':
            return self._get_by_products(where_filter)
