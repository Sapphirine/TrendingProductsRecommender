from flask_restful import Resource


class BaseResource(Resource):

    def __init__(self, **kwargs):
        self.spark_context = kwargs['spark_context']
        self.sql_context = kwargs['sql_context']
        self.catalog = kwargs['catalog']
        self.data_source_format = kwargs['data_source_format']
        self.table = kwargs['table']
        self.df = self.sql_context.read.options(catalog=self.catalog).format(self.data_source_format).load()

    def get(self, *args, **kwargs):
        raise NotImplementedError('BaseResource subclasses must implement get()')

    def __key(self):
        return self.spark_context, self.sql_context, self.catalog, self.data_source_format, self.table

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        return isinstance(self, type(other)) and self.__key() == other.__key()

    def __repr__(self):
        return str(self.__key())