__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

import pandas as pd
from mhs.src.dao.base_dao import BaseDAO, MetricsSharedCareAreaDocument
from mhs.src.shared_care_areas.metrics.base import BaseMetricsSharedCareArea, BaseMetricsNetwork
from mhs.src.dao.mhs.documents_mhs import NetworkHospitalDischargeDocument
import numpy as np
import logging


class NetworkBasedMetricsSharedCareArea(BaseMetricsSharedCareArea, BaseMetricsNetwork):

    def __init__(self, name):
        super(NetworkBasedMetricsSharedCareArea, self).__init__(name)
        self.__dao_network__ = BaseDAO(NetworkHospitalDischargeDocument)

    def get_metric(self, method, type_discharge, year, type_community_detection, normalized=None):
        df_shared_care_areas = self.get_shared_care_areas(method, type_community_detection,
                                                          type_discharge, year)
        df_network = self.get_network(method, type_discharge, year)
        df_sca = self.calculate_metric(df_network, df_shared_care_areas)

        if normalized:
            df_sca = self.get_normalized(df_sca, df_network, df_shared_care_areas)

        return df_sca

    def calculate_metric(self, df_network, df_shared_care_areas):
        raise NotImplemented()

    def get_normalized(self, df_sca, df_data, df_shared_care_areas):
        repetitions = 100
        # sca_r = list()
        sca_r = np.zeros(repetitions)
        for i in range(repetitions):
            df_shared_care_areas_r = df_shared_care_areas.copy()
            df_shared_care_areas_r['SCA_ID'] = np.random.permutation(df_shared_care_areas['SCA_ID'])
            df_sca_r = self.calculate_metric(df_data, df_shared_care_areas_r)
            sca_r[i] = np.mean(df_sca_r['metric_value'].values)
            del df_shared_care_areas_r
        sca_r = np.ravel(sca_r)
        # bootstraps_sca = f_bootstrap(df_sca['metric_value'].values)
        # bootstraps_sca_r = f_bootstrap(sca_r)
        mean_r = np.mean(sca_r)
        std_r = np.std(sca_r)
        col_metric_value = MetricsSharedCareAreaDocument.metric_value.name
        estimates = df_sca[col_metric_value]
        z_score = (estimates - mean_r) / std_r
        df_sca[col_metric_value] = z_score
        return df_sca


class ConductanceMetricsSharedCareArea(NetworkBasedMetricsSharedCareArea):
    NAME = 'NETWORK_CONDUCTANCE'

    def __init__(self):
        super().__init__(self.NAME)

    def calculate_metric(self, df_network, df_shared_care_areas):
        df_net_sca = df_network.merge(df_shared_care_areas, how='inner', left_on='NHD_NODE_IN', right_on='SCA_ZCTA')

        df_net_sca = df_net_sca.merge(df_shared_care_areas, how='inner', left_on='NHD_NODE_OUT', right_on='SCA_ZCTA',
                                      suffixes=('_NODE_IN', '_NODE_OUT'))

        df_sca_sca = df_net_sca.groupby([
            'SCA_ID_NODE_IN',
            'SCA_ID_NODE_OUT'
        ]).sum().reset_index()
        del df_net_sca

        df_sca_total = df_sca_sca.groupby(['SCA_ID_NODE_IN']).sum()

        df_sca_other = df_sca_sca \
            .loc[df_sca_sca['SCA_ID_NODE_IN'] != df_sca_sca['SCA_ID_NODE_OUT']] \
            .drop('SCA_ID_NODE_OUT', axis=1) \
            .groupby('SCA_ID_NODE_IN') \
            .sum()

        del df_sca_sca

        col_net_weight = NetworkHospitalDischargeDocument.weight
        df_sca_total = df_sca_total.join(df_sca_other, lsuffix='_TOTAL', rsuffix='_OTHER')
        df_sca_total[MetricsSharedCareAreaDocument.metric_value.name] = df_sca_total[
                                                                            f'{col_net_weight.db_field}_OTHER'] / \
                                                                        df_sca_total[f'{col_net_weight.db_field}_TOTAL']
        df_sca_total = df_sca_total.reset_index()
        df_sca_total = df_sca_total.rename(columns={
            'SCA_ID_NODE_IN': MetricsSharedCareAreaDocument.sca_id.name,
        })

        df_sca_total = self.get_all_scas(df_shared_care_areas, df_sca_total)

        df_sca_total = df_sca_total.fillna(1)

        return df_sca_total


class NumberCommunitiesMetricsSharedCareArea(NetworkBasedMetricsSharedCareArea):
    NAME = 'NUMBER_COMMUNITIES'

    def __init__(self):
        super().__init__(self.NAME)

    def calculate_metric(self, df_network, df_shared_care_areas):
        community_number = len(df_shared_care_areas['SCA_ID'].unique())

        col_name = MetricsSharedCareAreaDocument.sca_id.name
        col_value = MetricsSharedCareAreaDocument.metric_value.name
        df = pd.DataFrame.from_dict(data={col_name: ['0'], col_value: [community_number]})

        # logging.info(df.info())
        return df
