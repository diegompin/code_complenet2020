__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

import configparser
import os


class MHSConfigManager(object):

    def __init__(self):
        self._config = configparser.ConfigParser()

        # self.config.read('./mhs/mhs.cfg')
        # config_file = os.environ['MHS_CONFIG']
        self.config.read(f'{os.getcwd()}/mhs/mhs.cfg')
        # self.config.read(f'{os.getcwd()}/mhs/mhs.cfg')

    @property
    def mongo(self):
        config_mongo = self.config['mhs_mongo']
        database = config_mongo['database']
        return database

    @property
    def datalink_root(self):
        config_datalink = self.config['datalink']
        datalink_root = config_datalink['datalink_root']
        return datalink_root

    @property
    def data_aquisition_folder(self):
        data_acquisition = self.config['data_acquisition']['root_folder']
        return f'{self.datalink_root}/{data_acquisition}'

    @property
    def data_preparation_folder(self):
        data_preparation = self.config['data_preparation']['root_folder']
        return f'{self.datalink_root}/{data_preparation}'

    @property
    def data_curation(self):
        folder = self.config['data_curation']['root_folder']
        return f'{self.datalink_root}/{folder}'

    @property
    def plots(self):
        folder = self.config['plots']['root_folder']
        return f'{self.datalink_root}/{folder}'

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        self._config = value


'''
self = MHSConfigManager()
self.datalink_root
'''
