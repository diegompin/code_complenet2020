__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

import pandas as pd
from mhs.src.library.network.network_utils import *
from mhs.src.shared_care_areas.networks.network_shared_care import NetworkSharedCare
import networkx as nx
from mhs.src.library.network.community_detection import CommunityDetection
from mhs.src.library.network.network_metric import NetworkMetric, NetworkMetricFactory
from mhs.src.dao.mhs.documents_mhs import (
    HospitalDischargeDocument,
    SharedCareArea,
    MetricsSharedCareAreaDocument,
    NetworkHospitalDischargeDocument,
    MetricsNetworkSharedCareAreaDocument
)

from mhs.src.dao.base_dao import BaseDAO
from mhs.src.shared_care_areas.metrics.metrics import MetricsSharedCareAreaFactory
from mhs.src.shared_care_areas.plots.plot_map import PlotSCA


class SharedCareAreaScript(object):
    TYPE_NETWORK = 'NETWORKS'
    TYPE_NETWORK_METRICS = 'NETWORK_METRICS'
    TYPE_COMMUNITIES = 'COMMUNITIES'
    TYPE_COMMUNITY_MAPS = 'COMMUNITY_MAPS'
    TYPE_COMMUNITY_METRICS = 'COMMUNITY_METRICS'

    def get_script_class(self):
        return 'SHARED_CARE_AREA'

    def get_shared_care(self, args):
        sca_dict, item = args
        sca_obj = sca_dict.copy()
        sca_obj.update({
            SharedCareArea.zcta.name: str(item['NODE_ID']),
            SharedCareArea.sca_id.name: str(item['COMMUNITY_ID'])
        })
        return SharedCareArea(**sca_obj)

    def get_metrics_network_shared_care(self, args):
        metric_net_dict, item = args
        metric_net_obj = metric_net_dict.copy()
        metric_net_obj.update(item)
        return MetricsNetworkSharedCareAreaDocument(**metric_net_obj)

    def get_metrics_shared_care(self, args):
        sca_dict, item = args
        sca_obj = sca_dict.copy()
        sca_obj.update(item)
        # sca_obj = SharedCareArea.from_json(json.dumps(sca_dict))

        return MetricsSharedCareAreaDocument(**sca_obj)

    def get_operator_folder_path(self, name):
        config = MHSConfigManager()
        folder_root = config.data_curation
        folder_class = self.get_script_class()
        folder_name = name
        folder_path = f'{folder_root}/{folder_class}/{folder_name}'
        check_folder(folder_path)
        return folder_path

    def operator_create_network(self, method, year, discharge_type):
        folder_path = self.get_operator_folder_path(SharedCareAreaScript.TYPE_NETWORK)

        filename = f'{method}_{year}_{discharge_type}'
        filepath = f'{folder_path}/{filename}'

        net = NetworkSharedCare.instance(method)
        dict_match = {
            HospitalDischargeDocument.discharge_year: year,
            HospitalDischargeDocument.discharge_type: discharge_type
        }
        df = net.create_network(dict_match)

        df.to_csv(f'{filepath}.csv', index=False)
        df.to_hdf(f'{filepath}.hdf', key='df')
        G = build_from_dataframe(df)
        write_graph(G, folder=folder_path, filename=filename)
        del G

        dict_network = {
            NetworkHospitalDischargeDocument.network_method.name: method,
            NetworkHospitalDischargeDocument.network_year.name: year,
            NetworkHospitalDischargeDocument.network_type.name: discharge_type
        }
        dao = BaseDAO(NetworkHospitalDischargeDocument)
        dao.delete(**dict_network)
        #
        df = df.rename(columns={
            NetworkHospitalDischargeDocument.node_in.db_field: NetworkHospitalDischargeDocument.node_in.name,
            NetworkHospitalDischargeDocument.node_out.db_field: NetworkHospitalDischargeDocument.node_out.name,
            NetworkHospitalDischargeDocument.weight.db_field: NetworkHospitalDischargeDocument.weight.name,
        })
        df[NetworkHospitalDischargeDocument.network_method.name] = method
        df[NetworkHospitalDischargeDocument.network_year.name] = year
        df[NetworkHospitalDischargeDocument.network_type.name] = discharge_type
        # list_objects = list(
        #     map(self.get_object_network, [(dict_network, row) for (index, row) in df.iterrows()]))
        #
        # dao.insert(list_objects)

        dao.init_bulk(max_bulk_insert=100000)
        for (i, d) in df.iterrows():
            # (i, d) = next(df.iterrows())
            o = NetworkHospitalDischargeDocument(**d.to_dict())
            dao.insert_bulk(o)
        dao.exit_bulk()
        del df

        # NetworkHospitalDischargeDocument.objects.insert([NetworkHospitalDischargeDocument.from_json(json.dumps(d.to_dict())) for (i,d) in df.iterrows()], load_bulk=False)

    def operator_calculate_network_metrics(self, metric, method, year, discharge_type):
        dict_match = {
            NetworkHospitalDischargeDocument.network_method: method,
            NetworkHospitalDischargeDocument.network_type: discharge_type,
            NetworkHospitalDischargeDocument.network_year: year
        }
        dict_project = [
            # NetworkHospitalDischargeDocument.network_type,
            # NetworkHospitalDischargeDocument.network_year,
            NetworkHospitalDischargeDocument.node_in,
            NetworkHospitalDischargeDocument.node_out,
            NetworkHospitalDischargeDocument.weight,
        ]
        dao_network = BaseDAO(NetworkHospitalDischargeDocument)
        list_network = dao_network.obtain_pipeline(dict_match=dict_match,
                                           dict_project=dict_project,
                                           math_none=False)

        G = nx.Graph()
        G.add_weighted_edges_from((tuple(i.values()) for i in list_network))

        # list_metrics = NetworkMetricFactory.get_types()
        # for metric in list_metrics:
        mod_network_metric = NetworkMetricFactory.instance(metric)
        df_metric = mod_network_metric.get_metric(G)
        df_metric.columns = [MetricsNetworkSharedCareAreaDocument.metric_value.name]

        folder_path_out = self.get_operator_folder_path(SharedCareAreaScript.TYPE_NETWORK_METRICS)
        filename_out = f'{method}_{year}_{discharge_type}_{metric}'
        filepath_out = f'{folder_path_out}/{filename_out}'
        df_metric.to_csv(f'{filepath_out}.csv', index=False)

        metrics_dict = {
            MetricsNetworkSharedCareAreaDocument.method.name: method,
            MetricsNetworkSharedCareAreaDocument.year.name: year,
            MetricsNetworkSharedCareAreaDocument.type_discharge.name: discharge_type,
            MetricsNetworkSharedCareAreaDocument.metric.name: metric,
        }
        list_metrics = list(
            map(self.get_metrics_network_shared_care, [(metrics_dict, row) for (index, row) in df_metric.iterrows()]))
        dao_metrics = BaseDAO(MetricsNetworkSharedCareAreaDocument)
        dao_metrics.delete(**metrics_dict)
        dao_metrics.insert(list_metrics)

    def operator_extract_communities(self, method, year, discharge_type, type_community_detection):
        folder_path_in = self.get_operator_folder_path(SharedCareAreaScript.TYPE_NETWORK)
        filename_in = f'{method}_{year}_{discharge_type}'
        filepath_in = f'{folder_path_in}/{filename_in}'

        folder_path_out = self.get_operator_folder_path(SharedCareAreaScript.TYPE_COMMUNITIES)
        filename_out = f'{method}_{year}_{discharge_type}_{type_community_detection}'
        filepath_out = f'{folder_path_out}/{filename_out}'

        # exists = os.path.exists(f'{filepath_out}.csv')

        # if exists and not drop:
        #     return

        G = nx.read_pajek(f'{filepath_in}.pajek')
        community_detection = CommunityDetection.build_community_detection(type_community_detection)
        df_communities = community_detection.find_communities((G, filepath_in, filename_in))
        df_communities.rename(columns={
            SharedCareArea.zcta.db_field: 'NODE_ID',
            SharedCareArea.sca_id.db_field: 'COMMUNITY_ID'
        })

        df_communities.to_csv(f'{filepath_out}.csv', index=False)
        df_communities.to_hdf(f'{filepath_out}.hdf', 'df')

        sca_dict = {
            SharedCareArea.method.name: method,
            SharedCareArea.year.name: year,
            SharedCareArea.type_discharge.name: discharge_type,
            SharedCareArea.type_community_detection.name: type_community_detection
        }

        dao = BaseDAO(SharedCareArea)
        dao.delete(**sca_dict)

        list_shared_care_areas = list(
            map(self.get_shared_care, [(sca_dict, row) for (index, row) in df_communities.iterrows()]))

        dao.insert(list_shared_care_areas)

    def operator_calculate_metrics(self, method, year, discharge_type, type_community_detection, metric):
        dao = BaseDAO(MetricsSharedCareAreaDocument)

        folder_path_out = self.get_operator_folder_path(SharedCareAreaScript.TYPE_COMMUNITY_METRICS)

        # logging.info(f'{metric}')

        self.calculate_metrics_by_normalization(dao, discharge_type, folder_path_out, method, metric,
                                                type_community_detection, year, normalized=False)
        # self.calculate_metrics_by_normalization(dao, discharge_type, folder_path_out, method, metric,
        #                                         type_community_detection, year, normalized=True)

    def calculate_metrics_by_normalization(self, dao, discharge_type, folder_path_out, method, metric,
                                           type_community_detection, year, normalized):
        metrics_dict = {
            MetricsSharedCareAreaDocument.method.name: method,
            MetricsSharedCareAreaDocument.year.name: year,
            MetricsSharedCareAreaDocument.type_discharge.name: discharge_type,
            MetricsSharedCareAreaDocument.type_community_detection.name: type_community_detection,
            MetricsSharedCareAreaDocument.normalized.name: normalized
        }
        mod_metric = MetricsSharedCareAreaFactory.instance(metric)
        df = mod_metric.get_metric(**metrics_dict)
        filename_out = f'{method}_{year}_{discharge_type}_{type_community_detection}_{metric}_{normalized}'
        filepath_out = f'{folder_path_out}/{filename_out}'
        df.to_csv(f'{filepath_out}.csv', index=False)
        # df.to_hdf(f'{filepath_out}.hdf', 'df')
        metrics_dict = {
            MetricsSharedCareAreaDocument.method.name: method,
            MetricsSharedCareAreaDocument.year.name: year,
            MetricsSharedCareAreaDocument.type_discharge.name: discharge_type,
            MetricsSharedCareAreaDocument.type_community_detection.name: type_community_detection,
            MetricsSharedCareAreaDocument.metric.name: metric,
            MetricsSharedCareAreaDocument.normalized.name: normalized
        }
        list_metrics = list(map(self.get_metrics_shared_care, [(metrics_dict, row) for (index, row) in df.iterrows()]))
        dao.delete(**metrics_dict)
        dao.insert(list_metrics)

    def operator_plot_map(self, method, year, discharge_type, type_community_detection):
        plot_sca = PlotSCA()
        folder_path_out = self.get_operator_folder_path(SharedCareAreaScript.TYPE_COMMUNITY_MAPS)
        plot_sca.plot(method=method,
                      year=year,
                      type_discharge=discharge_type,
                      type_community_detection=type_community_detection,
                      path_plots=folder_path_out)
