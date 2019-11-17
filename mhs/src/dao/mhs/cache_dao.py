__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'
from mhs.src.dao.base_dao import *


class CacheDAO(BaseDAO):

    def __init__(self, cls):
        super().__init__(cls)
        self.collection = None

    def obtain_cache(self):
        if self.collection is None:
            results = self.obtain_pipeline({})
            df = pd.DataFrame.from_dict(results)
            self.collection = df
        return self.collection

