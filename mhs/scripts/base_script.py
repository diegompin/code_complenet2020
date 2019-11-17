# __author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.config import MHSConfigManager
import os
import urllib.request
import logging
from mhs.src.library.file.file_reader import FileReaderFactory
import json
from mhs.src.dao.mhs.dao_mongo import DAOMongo
from mhs.scripts.acquisition_script.script_acquisition import AcquisitionScript


class MHSScript(object):

    def execute(self, *args, **kwargs):
        pass


class BaseAcquisitionScript(MHSScript):

    def __init__(self, script: AcquisitionScript, plugins=None):
        self.script = script
        # self.plugins = OrderedDict()
        # if plugins:
        #     self.add_plugins(*plugins)
        self._config_manager = MHSConfigManager()
        self._log = logging.getLogger(__name__)

    @property
    def name(self):
        return self.__class__.__name__

    # def get_name(self):
    #     return 'MHSScriptScript'

    @property
    def log(self):
        return self._log

    @property
    def config_manager(self):
        return self._config_manager

    @property
    def folder_in(self):
        raise NotImplemented()

    @property
    def folder_out(self):
        raise NotImplemented()

    def get_file_name(self, *args, **kwargs):
        config_key = kwargs['config_key']
        config_name = kwargs['config_name']
        config_class = kwargs['config_class']
        filename = f'{config_class}_{config_name}_{config_key}'
        return filename

    def get_folder_out(self, *args, **kwargs):
        folder_out = self.folder_out
        config_class = kwargs['config_class']
        config_name = kwargs['config_name']

        return f'{folder_out}/{config_class}/{config_name}'

    def get_folder_in(self, *args, **kwargs):
        folder_in = self.folder_in
        config_class = kwargs['config_class']
        config_name = kwargs['config_name']
        return f'{folder_in}/{config_class}/{config_name}'

    def get_reader_type(self):
        return self.script.get_reader_type()

    def execute(self, *args, **kwargs):
        self.log.info("execute")
        config_class = kwargs['config_class']
        config_name = kwargs['config_name']

        def check_folder(folder):
            if not os.path.isdir(f'{self.folder_out}/{folder}'):
                os.makedirs(f'{self.folder_out}/{folder}')

        try:
            check_folder(config_class)
            check_folder(f'{config_class}/{config_name}')

        except Exception as e:
            # TODO Implement system-wide exception handling
            print(e)

    # def add_plugin(self, plugin):
    #     """Add a single solver plugin.
    #     If plugins have the same name, only the last one added is kept.
    #     """
    #     self.add_plugins(plugin)

    # def add_plugins(self, *plugins):
    #     """Add one or more solver plugins."""
    #     for plugin in plugins:
    #         plugin.initialize(self)
    #         self.plugins[plugin.__class__.__qualname__] = plugin
    #
    # def get_plugins(self):
    #     return self.plugins.values()
    #
    # def _call_plugins(self, hook, **kwargs):
    #     # should_stop = False
    #     for plugin in self.get_plugins():
    #         try:
    #             plugin(hook, **kwargs)
    #         except Exception:
    #             raise Exception()
    #             # should_stop = True
    #     # return should_stop

    def __call__(self, *args, **kwargs):
        self.log.info("__call__")
        self.execute(*args, **kwargs)


class DataAcquisitionScript(BaseAcquisitionScript):

    def __init__(self, script: AcquisitionScript):
        # self._folder_class = folder_class
        super().__init__(script)

    @property
    def folder_out(self):
        return f'{self.config_manager.data_aquisition_folder}'

    def execute(self, *args, **kwargs):
        super().execute(*args, **kwargs)
        out = self.get_folder_out(*args, **kwargs)
        url = kwargs['config_value']['URL']
        filename = self.get_file_name(*args, **kwargs)
        extension = self.get_reader_type()
        filename = f'{filename}'

        try:
            # self._call_plugins('pre_acquisition', **kwargs)
            # self._call_plugins('acquisition_script', **kwargs)
            urllib.request.urlretrieve(url=url, filename=f'{out}/{filename}.{extension}')
            # if self.script.is_zip():
            #     os.rename(f'{out}/{filename}.{extension}', f'{out}/{filename}.zip')
            self.script.on_post_acquire(self, **kwargs)
            # self._call_plugins('post_acquisition')
        except Exception as e:
            # TODO Implement system-wide exception handling
            print(e)


class DataPreparationScript(BaseAcquisitionScript):

    def __init__(self, script: AcquisitionScript):
        super().__init__(script)

    def get_folder_class(self):
        raise NotImplemented('get_folder_class not implemented')

    @property
    def folder_in(self):
        return self.config_manager.data_aquisition_folder

    @property
    def folder_out(self):
        return self.config_manager.data_preparation_folder

    def get_file_out_extension(self):
        return 'json'

    # def get_mapping(self):
    #     return self.script.get_fields()

    def get_reader(self):
        file_factory = FileReaderFactory()
        reader = file_factory.get_reader(self.get_reader_type())
        # reader.get_fields = self.get_mapping
        return reader

    def execute(self, *args, **kwargs):
        super().execute(*args, **kwargs)
        folder_out = self.get_folder_out(*args, **kwargs)

        try:
            file_in_name = self.get_file_name(*args, **kwargs)
            file_kwargs = self.get_file_kwargs(file_in_name, *args, **kwargs)
            # logging.info(file_in_name)

            file_kwargs['fields'] = self.script.get_fields(**kwargs)
            reader = self.get_reader()
            file = reader.read(**file_kwargs)
            file_out_externsion = self.get_file_out_extension()
            # self._call_plugins('pre_preparation')
            with open(f'{folder_out}/{file_in_name}.{file_out_externsion}', 'w') as outfile:
                for item in file:
                    # self._call_plugins('each_preparation', item=item)
                    prepared_item = self.script.get_prepared_item(self, item, **kwargs)
                    # outfile.write(f'{json.dumps(prepared_item)}\n')
                    if prepared_item:
                        outfile.write(f'{prepared_item.to_json()}\n')
            # self._call_plugins('post_preparation', **kwargs)

        except Exception as e:
            # TODO Implement system-wide exception handling
            # print(e)
            raise e

    def get_file_kwargs(self, file_in_name, *args, **kwargs):
        folder_in = self.get_folder_in(*args, **kwargs)
        file_in_extension = self.get_reader_type()
        file_kwargs = dict()
        file_kwargs['file_name'] = f'{folder_in}/{file_in_name}.{file_in_extension}'
        sheet_name = 'Sheet1'
        if 'sheet_name' in kwargs['config_value']:
            sheet_name = kwargs['config_value']['sheet_name']
        file_kwargs['sheet_name'] = sheet_name
        return file_kwargs


class MongoScript(BaseAcquisitionScript):

    def __init__(self, script: AcquisitionScript):
        self.documents = DAOMongo.instance()
        super().__init__(script)

    @property
    def folder_in(self):
        return self.config_manager.data_preparation_folder

    def get_file_in_extension(self):
        return 'json'

    def execute(self, *args, **kwargs):
        folder_in = self.get_folder_in(*args, **kwargs)
        errors = []
        try:
            filename = self.get_file_name(*args, **kwargs)
            file_in_extension = self.get_file_in_extension()
            self.script.on_pre_mongo(self, **kwargs)
            with open(f'{folder_in}/{filename}.{file_in_extension}', 'r') as infile:
                for line in infile:
                    dict_document = json.loads(line)
                    self.mongo_command(dict_document)
                    # self.documents.insert_inpatient_discharge(dict_document)
            self.script.on_post_mongo(self, **kwargs)
        except Exception as e:
            raise Exception(e)
            # errors.append(e)
        if errors:
            raise Exception(errors)
            # self.log.error(errors)

    def mongo_command(self, dict_document):
        self.script.mongo_command(dict_document)
#
#
# class DataPreparationScriptOld(BaseScript):
#
#     def __init__(self):
#         # self._folder_class = folder_class
#         super().__init__()
#         # self._folder_out = self.config_manager.data_aquisition_folder
#
#     def get_folder_class(self):
#         raise NotImplemented('get_folder_class not implemented')
#
#     def get_folder_in(self):
#         return self.config_manager.data_aquisition_folder
#
#     def get_folder_out(self):
#         return self.config_manager.data_preparation_folder
#
#     def get_file_in_extension(self):
#         raise NotImplemented('get_file_in_extension not implemented')
#
#     def get_file_out_extension(self):
#         return 'json'
#
#     def get_reader_type(self):
#         pass
#
#     def get_mapping(self):
#         pass
#
#     def get_reader(self):
#         file_factory = FileReaderFactory()
#         reader = file_factory.get_reader(self.get_reader_type())
#         reader.get_mapping = self.get_mapping
#         return reader
#
#     def execute(self, *args, **kwargs):
#         super().execute(*args, **kwargs)
#         folder_out = self.folder_out
#         folder_in = self.folder_in
#         try:
#             file_in_name = kwargs['file_name']
#             file_in_extension = self.get_file_in_extension()
#             kwargs['file_name'] = f'{folder_in}/{file_in_name}.{file_in_extension}'
#             reader = self.get_reader()
#             file = reader.read(*args, **kwargs)
#             file_out_externsion = self.get_file_out_extension()
#             with open(f'{folder_out}/{file_in_name}.{file_out_externsion}', 'w') as outfile:
#                 for item in file:
#                     outfile.write(f'{json.dumps(item)}\n')
#                     # outfile.writelines(json.dumps(item))
#                     # json.dump(item, outfile)
#                     # break
#
#         except Exception as e:
#             # TODO Implement system-wide exception handling
#             print(e)

#
# class DataAcquisitionScriptOld(BaseScript):
#
#     def __init__(self):
#         # self._folder_class = folder_class
#         super().__init__(None)
#         # self._folder_out = self.config_manager.data_aquisition_folder
#
#     def get_folder_class(self):
#         raise NotImplemented('get_folder_class not implemented')
#
#     def get_folder_out(self):
#         return self.config_manager.data_aquisition_folder
#
#     # @property
#     # def folder_out(self):
#     #     self.log.info('Entering folder_out')
#     #     folder_class = self.get_folder_class()
#     #     folder_out = self.config_manager.data_aquisition_folder
#     #     return f'{folder_out}/{folder_class}'
#
#     def execute(self, *args, **kwargs):
#         super().execute(*args, **kwargs)
#         out = self.folder_out
#         filename = kwargs['file_name']
#         url = kwargs['url']
#         # if 'extension' in kwargs:
#         #     extension = kwargs['extension']
#
#         extension = self.get_file_out_extension()
#         filename = f'{filename}.{extension}'
#         try:
#             urllib.request.urlretrieve(url=url, filename=f'{out}/{filename}')
#             # wget.download(out=f'{out}/', )
#         except Exception as e:
#             # TODO Implement system-wide exception handling
#             print(e)
