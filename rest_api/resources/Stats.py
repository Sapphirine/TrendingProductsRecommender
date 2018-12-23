from flask_restful import abort
from Memoized import Memoized
from rest_api.resources.BaseResource import BaseResource
import json


DEFAULT_WHERE_CLAUSE = ('sentiment = True and (favorite_count > 0 or retweet_count > 0 '
                        'or quote_count > 0 or reply_count > 0)')


class Summary(BaseResource):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.terms = kwargs['terms']

    @Memoized
    def get(self, cache_clear=None):
        return {
            'total_tweets': self.df.count(),
            'num_categories': self.df.select('category').distinct().count(),
            'num_products': self.df.select('product').distinct().count(),
            'num_search_phrases': sum([len(self.terms[cat]) for cat in self.terms]),
            'table_name': self.table
        }


class Count(BaseResource):

    @Memoized
    def get(self, by_type: str, where_filter: str = None, cache_clear=None):
        try:
            if where_filter and where_filter != 'none':
                counts = self.df.filter(where_filter).groupBy(by_type).count().toJSON().collect()
            else:
                counts = self.df.groupBy(by_type).count().toJSON().collect()
            ret = {}
            for count in counts:
                j = json.loads(count)
                ret[j[by_type]] = j['count']
            return ret
        except Exception:
            abort(404, message="could not complete count query grouped by {}".format(by_type))
