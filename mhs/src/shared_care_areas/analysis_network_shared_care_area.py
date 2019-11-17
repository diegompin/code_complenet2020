__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.base_dao import BaseDAO
import pandas as pd
from mhs.src.dao.mhs.documents_mhs import NetworkHospitalDischargeDocument
from mhs.src.dao.mhs.documents_mhs import HospitalDischargeDocument, SharedCareArea

mod_network = BaseDAO(NetworkHospitalDischargeDocument)
mod_sca = BaseDAO(SharedCareArea)

col_net_method = NetworkHospitalDischargeDocument.network_method
col_net_dicharge = NetworkHospitalDischargeDocument.network_type
col_net_year = NetworkHospitalDischargeDocument.network_year
col_net_node_in = NetworkHospitalDischargeDocument.node_in
col_net_node_out = NetworkHospitalDischargeDocument.node_out
col_net_weight = NetworkHospitalDischargeDocument.weight

from mhs.src.shared_care_areas.networks.network_shared_care import NetworkSharedCare
from mhs.src.library.network.community_detection import CommunityDetection

value_method = NetworkSharedCare.TYPE_PINHEIRO
value_discharge = HospitalDischargeDocument.TYPE_DISCHARGE_ED_ONLY
value_type_community_detection = CommunityDetection.TYPE_INFOMAP
value_year = 2014

df = pd.DataFrame.from_dict(mod_network.obtain_pipeline(
    dict_match={
        col_net_method: value_method,
        col_net_dicharge: value_discharge,
        col_net_year: value_year
    },
    dict_project={
        # col_method,
        # col_dicharge,
        # col_year,
        col_net_node_in,
        col_net_node_out,
        col_net_weight
    },
))

col_sca_method = SharedCareArea.method
col_sca_type_discharge = SharedCareArea.type_discharge
col_sca_type_community_detection = SharedCareArea.type_community_detection
col_sca_year = SharedCareArea.year
col_sca_zcta = SharedCareArea.zcta
col_sca_sca_id = SharedCareArea.sca_id

df_sca = pd.DataFrame.from_dict(mod_sca.obtain_pipeline(
    dict_match={
        col_sca_method: value_method,
        col_sca_type_discharge: value_discharge,
        col_sca_type_community_detection: value_type_community_detection,
        col_sca_year: value_year
    },
    dict_project={
        col_sca_zcta,
        col_sca_sca_id
    },

))

df_net_sca = df.merge(df_sca, how='inner', left_on='NHD_NODE_IN', right_on='SCA_ZCTA')
df_net_sca = df_net_sca.merge(df_sca, how='inner', left_on='NHD_NODE_OUT', right_on='SCA_ZCTA',
                      suffixes=('_NODE_IN', '_NODE_OUT'))

df_net_sca_group = df_net_sca.groupby(col_sca_sca_id.db_field)

df_net_sca_group.apply(lambda x: x[col_net_weight.db_field])
