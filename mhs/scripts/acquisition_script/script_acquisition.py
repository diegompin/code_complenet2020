__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.dao_mongo import DAOMongo


# from mhs.scripts.mhs_script import DataPreparationScript

#
# class MHSScriptNew(object):
#
#     def __init__(self):
#
#

class AcquisitionScript(object):

    def __init__(self):
        self.documents = DAOMongo.instance()
        super().__init__()

    @staticmethod
    def instance(**kwargs):
        pass
        # self.script_acquisition = DataPreparationScript()

    def is_zip(self):
        return False

    def get_fields(self, **kwargs):
        raise NotImplemented()

    def get_reader_type(self):
        raise NotImplemented()

    def mongo_command(self, dict_document):
        raise NotImplemented()

    def on_post_acquire(self, script, **kwargs):
        pass

    def on_pre_mongo(self, script, **kwargs):
        pass

    def on_post_mongo(self, script, **kwargs):
        pass

    def get_prepared_item(self, script, item,  **kwargs):
        raise NotImplemented()

    def get_plugins(self):
        pass

