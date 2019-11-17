__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

import pandas as pd
from mhs.src.dao.mhs.documents_mhs import SharedCareArea, MetricsSharedCareAreaDocument, \
    NetworkHospitalDischargeDocument
from mhs.src.dao.base_dao import BaseDAO


class BaseMetricsNetwork(object):

    def __init__(self, name):
        self.name = name
        self.__dao_network__ = BaseDAO(NetworkHospitalDischargeDocument)

    def get_metric(self, method, type_discharge, year, normalized=None):
        raise NotImplemented('get_metric')

    def get_network(self, method, type_discharge, year):
        col_net_method = NetworkHospitalDischargeDocument.network_method
        col_net_dicharge = NetworkHospitalDischargeDocument.network_type
        col_net_year = NetworkHospitalDischargeDocument.network_year
        col_net_node_in = NetworkHospitalDischargeDocument.node_in
        col_net_node_out = NetworkHospitalDischargeDocument.node_out
        col_net_weight = NetworkHospitalDischargeDocument.weight

        df = pd.DataFrame.from_dict(self.__dao_network__.obtain_pipeline(
            dict_match={
                col_net_method: method,
                col_net_dicharge: type_discharge,
                col_net_year: year
            },
            dict_project={
                col_net_method,
                col_net_dicharge,
                col_net_year,
                col_net_node_in,
                col_net_node_out,
                col_net_weight
            },
        ))

        return df


class BaseMetricsSharedCareArea(object):

    def __init__(self, name):
        self.name = name
        self.__dao_shared_care_area__ = BaseDAO(SharedCareArea)

    def get_metric(self, method, type_discharge, year, type_community_detection, normalized=None):
        raise NotImplemented('get_metric')

    def get_shared_care_areas(self, method, type_community_detection, type_discharge, year):
        sca_match_dict = {
            SharedCareArea.method: method,
            SharedCareArea.type_community_detection: type_community_detection,
            SharedCareArea.year: year,
            SharedCareArea.type_discharge: type_discharge
        }
        sca_project = [
            SharedCareArea.zcta,
            SharedCareArea.sca_id
        ]
        list_shared_care_areas = self.__dao_shared_care_area__.obtain_pipeline(sca_match_dict, sca_project,
                                                                               math_none=True)
        df_shared_care_areas = pd.DataFrame.from_dict(list_shared_care_areas)
        return df_shared_care_areas

    def get_all_scas(self, df_shared_care_areas, df_sca_metric):
        df_result = pd.DataFrame()
        df_result[MetricsSharedCareAreaDocument.sca_id.name] = df_shared_care_areas['SCA_ID'].unique()
        df_result = df_result.merge(df_sca_metric, how='left')
        # df_result = df_result.fillna(0)
        df_result = df_result[[
            MetricsSharedCareAreaDocument.sca_id.name,
            MetricsSharedCareAreaDocument.metric_value.name
        ]]
        return df_result
