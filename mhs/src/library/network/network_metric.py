__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

import pandas as pd
from mhs.src.dao.mhs.documents_mhs import MetricsNetworkSharedCareAreaDocument
import networkx as nx


class NetworkMetricFactory(object):
    TYPE_NETWORK_NODES_TOTAL = 'NETWORK_NODES_TOTAL'
    TYPE_NETWORK_LINKS_TOTAL = 'NETWORK_LINKS_TOTAL'
    TYPE_LINKS_WEIGHT_TOTAL = 'LINKS_WEIGHT_TOTAL'
    TYPE_NETWORK_DENSITY_UNWEIGHTED = 'NETWORK_DENSITY_UNWEIGHTED'
    TYPE_NODE_CLUSTERING_UNWEIGHTED_AVERAGE = 'NODE_CLUSTERING_UNWEIGHTED_AVERAGE'
    TYPE_NODE_CLUSTERING_WEIGHTED_AVERAGE = 'NODE_CLUSTERING_WEIGHTED_AVERAGE'
    TYPE_NODE_SHORTEST_PATH_WEIGHTED_AVERAGE = 'NODE_SHORTEST_PATH_WEIGHTED_AVERAGE'
    TYPE_NODE_SHORTEST_PATH_UNWEIGHTED_AVERAGE = 'NODE_SHORTEST_PATH_UNWEIGHTED_AVERAGE'
    TYPE_NODE_DEGREE_UNWEIGHTED_AVERAGE = 'NODE_DEGREE_UNWEIGHTED_AVERAGE'
    TYPE_NODE_DEGREE_WEIGHTED_AVERAGE = 'NODE_DEGREE_WEIGHTED_AVERAGE'

    TYPES = [
        TYPE_NETWORK_NODES_TOTAL,
        TYPE_NETWORK_LINKS_TOTAL,
        TYPE_LINKS_WEIGHT_TOTAL,
        TYPE_NETWORK_DENSITY_UNWEIGHTED,
        TYPE_NODE_CLUSTERING_UNWEIGHTED_AVERAGE,
        TYPE_NODE_CLUSTERING_WEIGHTED_AVERAGE,
        TYPE_NODE_SHORTEST_PATH_WEIGHTED_AVERAGE,
        TYPE_NODE_SHORTEST_PATH_UNWEIGHTED_AVERAGE,
        TYPE_NODE_DEGREE_UNWEIGHTED_AVERAGE,
        TYPE_NODE_DEGREE_WEIGHTED_AVERAGE
    ]

    def __init__(self):
        pass

    @staticmethod
    def instance(metric):
        if metric == NetworkMetricFactory.TYPE_NETWORK_NODES_TOTAL:
            return NetworkNodesTotalNetworkMetric()
        elif metric == NetworkMetricFactory.TYPE_NETWORK_LINKS_TOTAL:
            return NetworkLinksTotalNetworkMetric()
        elif metric == NetworkMetricFactory.TYPE_LINKS_WEIGHT_TOTAL:
            return LinksWeightTotalNetworkMetric()
        elif metric == NetworkMetricFactory.TYPE_NODE_CLUSTERING_UNWEIGHTED_AVERAGE:
            return NodeClusteringUnweightedAverageNetworkMetric()
        elif metric == NetworkMetricFactory.TYPE_NODE_CLUSTERING_WEIGHTED_AVERAGE:
            return NodeClusteringWeightedAverageNetworkMetric()
        elif metric == NetworkMetricFactory.TYPE_NODE_SHORTEST_PATH_UNWEIGHTED_AVERAGE:
            return NodeShortestPathUnweightedAverageNetworkMetric()
        elif metric == NetworkMetricFactory.TYPE_NODE_SHORTEST_PATH_WEIGHTED_AVERAGE:
            return NodeShortestPathWeightedAverageNetworkMetric()
        elif metric == NetworkMetricFactory.TYPE_NETWORK_DENSITY_UNWEIGHTED:
            return NetworkDensityUnweightedNetworkMetric()
        elif metric == NetworkMetricFactory.TYPE_NODE_DEGREE_UNWEIGHTED_AVERAGE:
            return NodeDegreeUnweightedAverageNetworkMetric()
        elif metric == NetworkMetricFactory.TYPE_NODE_DEGREE_WEIGHTED_AVERAGE:
            return NodeDegreeWeightedAverageNetworkMetric()
        else:
            raise NotImplemented(metric)

    @staticmethod
    def get_types():
        return sorted(NetworkMetricFactory.TYPES)


class NetworkMetric(object):

    def __init__(self):
        # self.name = name
        pass

    def get_metric(self, graph):
        list_metrics = self.__calculate_metric__(graph)
        df_metric = self.prepare_result(list_metrics)
        return df_metric

    def __calculate_metric__(self, graph):
        raise NotImplemented

    def prepare_result(self, list_metrics):
        df = pd.DataFrame(list_metrics)
        df.columns = [MetricsNetworkSharedCareAreaDocument.metric.name]
        return df


class NetworkNodesTotalNetworkMetric(NetworkMetric):
    def __calculate_metric__(self, graph):
        N = graph.order()
        return [N]


class NetworkLinksTotalNetworkMetric(NetworkMetric):
    def __calculate_metric__(self, graph):
        m = graph.size()
        return [m]


class LinksWeightTotalNetworkMetric(NetworkMetric):
    def __calculate_metric__(self, graph):
        m = graph.size(weight='weight')
        return [m]


class NodeClusteringWeightedAverageNetworkMetric(NetworkMetric):
    def __calculate_metric__(self, graph):
        m = nx.average_clustering(graph, weight='weight')
        return [m]


class NodeClusteringUnweightedAverageNetworkMetric(NetworkMetric):
    def __calculate_metric__(self, graph):
        m = nx.average_clustering(graph)
        return [m]


class NodeShortestPathWeightedAverageNetworkMetric(NetworkMetric):
    def __calculate_metric__(self, graph):
        m = nx.average_shortest_path_length(graph, weight='weight')
        return [m]


class NodeShortestPathUnweightedAverageNetworkMetric(NetworkMetric):
    def __calculate_metric__(self, graph):
        m = nx.average_shortest_path_length(graph)
        return [m]


class NodeDegreeUnweightedAverageNetworkMetric(NetworkMetric):
    def __calculate_metric__(self, graph):
        N = graph.order()
        M = graph.size()
        return [M / N]


class NodeDegreeWeightedAverageNetworkMetric(NetworkMetric):
    def __calculate_metric__(self, graph):
        N = graph.order()
        M = graph.size(weight='weight')
        return [M / N]


class NetworkDensityUnweightedNetworkMetric(NetworkMetric):
    def __calculate_metric__(self, graph):
        m = nx.density(graph)
        return [m]

# W = G.size(weight='weight')
#
# avg_deg = float(K) / N
#
# avg_deg_wei = float(W) / N
#
#
