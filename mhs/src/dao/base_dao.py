__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

DOC_MATCH = '$match'
DOC_PROJECT = '$project'
DOC_GROUP = '$group'

from mhs.src.dao.mhs.dao_mongo import *


class BaseDAO(object):

    def __init__(self, cls):
        self.daomongo = DAOMongo.instance()
        self.cls = cls
        self.max_bulk_insert = 1000
        self.collection = []

    def build_pipeline(self, kwargs):
        dict_match = {}
        if kwargs:
            # dict_match = {DOC_MATCH: {}}
            for (k, v) in kwargs.items():
                if v:
                    dict_match[k] = v
        return dict_match

    def build_pipeline_match(self, kwargs, math_none=False):
        dict_match = {DOC_MATCH: {}}
        if kwargs:
            # dict_match = {DOC_MATCH: {}}
            for (k, v) in kwargs.items():
                if v or math_none:
                    dict_match[DOC_MATCH][k.db_field] = v
        return dict_match

    def build_project(self, args):
        dict_project = {DOC_PROJECT: {}}
        dict_project[DOC_PROJECT]['_id'] = False
        if args:
            for i in args:
                dict_project[DOC_PROJECT][i.db_field] = True
        return dict_project

    def get_type(self):
        return self.cls

    def obtain(self, dict_match):
        dict_match = self.build_pipeline(dict_match)

        self_type = self.get_type()
        result = getattr(self_type, 'objects')(**dict_match)
        # result = HospitalDischargeDocument
        return result

    def obtain_pipeline(self, dict_match, dict_project=None, math_none=False):
        list_pipeline = list()

        dict_match = self.build_pipeline_match(dict_match, math_none)
        list_pipeline.append(dict_match)
        if dict_project:
            dict_project = self.build_project(dict_project)

            list_pipeline.append(dict_project)

        self_type = self.get_type()
        objects = getattr(self_type, 'objects')
        result = objects.aggregate(*list_pipeline)
        # result = HospitalDischargeDocument.objects.aggregate(*list_pipeline)

        return result

    def insert(self, list_objects):
        # NetworkHospitalDischargeDocument._get_collection().insert_many(m)
        self_type = self.get_type()
        objects = getattr(self_type, 'objects')
        objects.insert(list_objects, load_bulk=False)
        # _get_collection = getattr(self_type, '_get_collection')
        # _get_collection().insert_many([o.to_mongo() for o in list_objects])

    def insert_bulk(self, object, **kwargs):
        self.collection.append(object)
        if len(self.collection) >= self.max_bulk_insert:
            self.insert(self.collection)
            self.collection = []

    def init_bulk(self, max_bulk_insert=None):
        self.collection = []
        if max_bulk_insert:
            self.max_bulk_insert = max_bulk_insert

    def exit_bulk(self):
        if len(self.collection) > 0:
            self.insert(self.collection)
            self.collection = []
        # self.collection.insert(object)
        # if self.collection:
        #     self.insert(self.collection)
        #     self.collection = []

    def delete(self, **kwargs):
        self_type = self.get_type()
        objects = getattr(self_type, 'objects')
        objects(**kwargs).delete()
