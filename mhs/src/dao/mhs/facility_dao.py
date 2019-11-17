__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.base_dao import *


class FacilityDAO(BaseDAO):

    def get_type(self):
        return FacilityDocument

