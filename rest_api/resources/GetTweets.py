import json
from rest_api.resources.BaseResource import BaseResource
from Memoized import Memoized


DEFAULT_MARKER = 'default'


class GetTweets(BaseResource):

    @Memoized
    def get(self, count: int = 20, sort: str = 'favorite_count', order: str = 'desc', category: str = None,
            cache_clear: str = None):
        if count == 0:
            count = 20
        if sort == DEFAULT_MARKER:
            sort = 'favorite_count'
        if order == DEFAULT_MARKER:
            order = 'desc'
        if category == DEFAULT_MARKER:
            category = None
        cols = ['row_id', 'category', 'text', 'retweet_count', 'favorite_count', 'product']
        query = self.df
        if category and category != 'all':
            query = query.where('category = "{}"'.format(category))
        query = query.orderBy(sort, ascending=not bool(order.lower() == 'desc'))
        query = query.limit(count)
        rows = []
        for r in query.select(cols).toJSON().collect():
            rows.append(json.loads(r))
        return rows
