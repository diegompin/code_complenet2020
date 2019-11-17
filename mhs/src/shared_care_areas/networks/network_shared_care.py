__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.documents_mhs import HospitalDischargeDocument
from mhs.src.dao.mhs.documents_mhs import NetworkHospitalDischargeDocument
from mhs.src.dao.base_dao import BaseDAO
import pandas as pd


class NetworkSharedCare(object):
    TYPE_HU = 'HU'

    LIST_TYPES = [TYPE_HU]

    def __init__(self):
        self.discharges = BaseDAO(HospitalDischargeDocument)

    @classmethod
    def instance(cls, network_type):
        if network_type == NetworkSharedCare.TYPE_HU:
            return HuNetworkSharedCare()
        else:
            raise NotImplemented(f'{network_type} not implemented')

    def get_nodes(self):
        pass

    def transform_network(self, df):
        return df

    def create_network(self, dict_match):
        results = self.discharges.obtain_pipeline(dict_match)
        nodes = self.get_nodes()
        df = pd.DataFrame.from_dict(results)
        df = df.groupby(nodes).sum()[HospitalDischargeDocument.discharge_quantity.db_field].reset_index()
        df = self.transform_network(df)
        df.columns = self.get_columns()
        return df

    def get_columns(self):
        return [
            NetworkHospitalDischargeDocument.node_in.db_field,
            NetworkHospitalDischargeDocument.node_out.db_field,
            NetworkHospitalDischargeDocument.weight.db_field,
        ]


class HuNetworkSharedCare(NetworkSharedCare):
    """
    Hu, Y., Wang, F., & Xierali, I. M. (2018).
    Automated Delineation of Hospital Service Areas and Hospital Referral Regions by Modularity Optimization.
    Health Services Research, 53(1), 236–255.
    http://doi.org/10.1111/1475-6773.12616
    """

    def __init__(self):
        super().__init__()

    def get_nodes(self):
        return [
            HospitalDischargeDocument.facility_zcta.db_field,
            HospitalDischargeDocument.patient_zcta.db_field
        ]