import infomap
import pandas as pd
import community
import networkx as nx
# from multiprocessing import Pool
import numpy as np
# from mhs.src.common.multiprocessing.util_multiprocessing import MyPool
from multiprocessing import Pool
import uuid
import pika
import json
from mhs.src.library.network.network_utils import check_folder
import subprocess
import os,glob

class CommunityDetection(object):
    TYPE_INFOMAP = 'INFOMAP'
    TYPE_LOUVAIN = 'LOUVAIN'
    TYPE_BLOCKMODEL = 'BLOCKMODEL'
    TYPE_LABELPROPAGATION = 'LABELPROPAGATION'
    TYPE_SLPA = 'SLPA'
    TYPES = [
        TYPE_INFOMAP,
        TYPE_LOUVAIN,
        TYPE_BLOCKMODEL,
        # TYPE_LABELPROPAGATION,
        TYPE_SLPA
    ]

    @staticmethod
    def get_types():
        return sorted(CommunityDetection.TYPES)

    def __init__(self):
        pass

    @staticmethod
    def build_community_detection(type_community_detection):
        if type_community_detection == CommunityDetection.TYPE_LOUVAIN:
            return LouvainCommunityDetection()
        elif type_community_detection == CommunityDetection.TYPE_INFOMAP:
            return InfomapCommunityDetection()
        elif type_community_detection == CommunityDetection.TYPE_BLOCKMODEL:
            return StochasticBlockModelCommunityDetection()
        elif type_community_detection == CommunityDetection.TYPE_LABELPROPAGATION:
            return LabelPropagationCommunityDetection()
        elif type_community_detection == CommunityDetection.TYPE_SLPA:
            return SLPACommunityDetection()
        else:
            raise NotImplemented(type_community_detection)

    def find_communities(self, graph, **kwargs):
        pass


class LabelPropagationCommunityDetection(CommunityDetection):

    def find_communities(self, graphPair, **kwargs):
        graph = graphPair[0]
        best_partition = nx.algorithms.community.label_propagation_communities(graph)

        # trials = 30
        # if kwargs:
        #     if 'trials' in kwargs:
        #         trials = kwargs['trials']
        #
        # pool = Pool(processes=1)
        # list_partition = pool.map(community.best_partition, [graph] * trials)
        # pool.close()
        # pool.join()
        #
        # best_modularity = -np.inf
        # best_partition = None
        # for partition in list_partition:
        #     partition_modularity = community.modularity(partition, graph)
        #     # print(partition_modularity)
        #     if partition_modularity > best_modularity:
        #         best_modularity = partition_modularity
        #         best_partition = partition
        # best_partition = community.best_partition(graph)

        dict_communities = {}
        community_id = 0
        for nodes in best_partition:
            for node in nodes:
                dict_communities[node] = community_id
            community_id += 1
        df_communities = pd.DataFrame(dict_communities.items())
        df_communities.columns = ['NODE_ID', 'COMMUNITY_ID']
        return df_communities


class InfomapCommunityDetection(CommunityDetection):

    def find_communities(self, graphPair, **kwargs):
        graph = graphPair[0]
        # infomap_command = f'--two-level -N {trials} --silent'
        # infomap_command = f'-N {trials} --silent'
        trials = 30
        if kwargs:
            if 'trials' in kwargs:
                trials = kwargs['trials']

        infomap_command = f'-N {trials} --two-level'
        infomapWrapper = infomap.Infomap(infomap_command)
        assert isinstance(infomapWrapper, infomap.Infomap)

        for e in graph.edges(data=True):
            try:
                infomapWrapper.addLink(int(e[0]), int(e[1]), e[2]['weight'])
            except Exception as e:
                pass

        infomapWrapper.run()

        infomapWrapper.getModules(level=2)
        infomapWrapper.getModules()

        dict_communities = {}
        for node in infomapWrapper.iterTree():
            if node.isLeaf():
                dict_communities[node.physicalId] = node.moduleIndex()
        df_communities = pd.DataFrame(list(dict_communities.items()))
        df_communities.columns = ['NODE_ID', 'COMMUNITY_ID']
        return df_communities


class LouvainCommunityDetection(CommunityDetection):

    def find_communities(self, graphPair, **kwargs):
        graph = graphPair[0]
        # trials = 30
        # if kwargs:
        #     if 'trials' in kwargs:
        #         trials = kwargs['trials']
        #
        # pool = Pool(processes=1)
        # list_partition = pool.map(community.best_partition, [graph] * trials)
        # pool.close()
        # pool.join()
        #
        # best_modularity = -np.inf
        # best_partition = None
        # for partition in list_partition:
        #     partition_modularity = community.modularity(partition, graph)
        #     # print(partition_modularity)
        #     if partition_modularity > best_modularity:
        #         best_modularity = partition_modularity
        #         best_partition = partition
        best_partition = community.best_partition(graph)

        dict_communities = {}
        for node_id, community_id in best_partition.items():
            dict_communities[node_id] = community_id
        df_communities = pd.DataFrame(list(dict_communities.items()))
        df_communities.columns = ['NODE_ID', 'COMMUNITY_ID']
        return df_communities


class StochasticBlockModelCommunityDetection(CommunityDetection):

    def __init__(self):
        self.credentials = pika.PlainCredentials(username='user', password='bitnami')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', credentials=self.credentials))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='StochasticBlockModel',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(n))
        while self.response is None:
            self.connection.process_data_events()
        return self.response

    def find_communities(self, graph, **kwargs):
        communityDict = self.call(graph[1])
        communityDict = json.loads(communityDict.decode('utf-8'))
        return pd.DataFrame.from_dict(communityDict)


class SLPACommunityDetection(CommunityDetection):
    def find_communities(self, graphPair, **kwargs):
        edge_list = graphPair[1] + '-edgeList.txt'
        dataset_name = graphPair[2]
        post_processing_value = '0.5'
        slpa_folder = '/usr/local/airflow/mhs/scripts/slpa/GANXiS_v3.0.2/'
        # check_folder(slpa_folder)
        slpa_output_folder = f'{slpa_folder}/output'
        check_folder(slpa_output_folder)
        jar_path = f'{slpa_folder}/GANXiSw.jar'
        community_file = f'SLPAw_{dataset_name}-edgeList_run1_r{post_processing_value}_v3_T100.icpm.node-com.txt'
        subprocess.call(['java', '-jar', jar_path, '-Sym', '1', '-i', edge_list, '-Onc', '1', '-d', slpa_output_folder])
        node_list = list()
        community_id_list = list()
        slpa_filepath = f'{slpa_output_folder}/{community_file}'
        with open(slpa_filepath) as slpa_output_file:
            for line in slpa_output_file:
                split = line.split(" ")
                node_list.append(split[0])
                community_id_list.append(split[1])
        community_mapping = dict()
        community_mapping['NODE_ID'] = node_list
        community_mapping['COMMUNITY_ID'] = community_id_list
        for filename in glob.glob(f'{slpa_output_folder}/SLPAw_{dataset_name}-edgeList_run1*'):
            os.remove(filename)
        # os.remove(slpa_filepath)
        return pd.DataFrame.from_dict(community_mapping)

    # @staticmethod
    # def find_communities_infomap(graph, trials=30):
    #     # infomap_command = f'--two-level -N {trials} --silent'
    #     # infomap_command = f'-N {trials} --silent'
    #     infomap_command = f'-N {trials} --two-level'
    #     infomapWrapper = infomap.Infomap(infomap_command)
    #     assert isinstance(infomapWrapper, infomap.Infomap)
    #
    #     for e in graph.edges(data=True):
    #         try:
    #             infomapWrapper.addLink(int(e[0]), int(e[1]), e[2]['weight'])
    #         except Exception as e:
    #             pass
    #
    #     infomapWrapper.run()
    #
    #     infomapWrapper.getModules(level=2)
    #     infomapWrapper.getModules()
    #
    #     dict_communities = {}
    #     for node in infomapWrapper.iterTree():
    #         if node.isLeaf():
    #             dict_communities[node.physicalId] = node.moduleIndex()
    #     df_communities = pd.DataFrame(list(dict_communities.items()))
    #     df_communities.columns = ['NODE_ID', 'COMMUNITY_ID']
    #     return df_communities
    #
    # @staticmethod
    # def find_communities_louvain(graph, trials=30):
    #
    #     with Pool(processes=trials) as pool:
    #         list_partition = pool.map(community.best_partition, [graph] * trials)
    #
    #     best_modularity = -np.inf
    #     best_partition = None
    #     for partition in list_partition:
    #         partition_modularity = community.modularity(partition, graph)
    #         print(partition_modularity)
    #         if partition_modularity > best_modularity:
    #             best_modularity = partition_modularity
    #             best_partition = partition
    #     # partition = community.best_partition(G)
    #
    #     dict_communities = {}
    #     for node_id, community_id in best_partition.items():
    #         dict_communities[node_id] = community_id
    #     df_communities = pd.DataFrame(list(dict_communities.items()))
    #     df_communities.columns = ['NODE_ID', 'COMMUNITY_ID']
    #     return df_communities
    #
    # @staticmethod
    # def extract_communities(graph, method='infomap'):
    #     possible_methods = {
    #         'infomap': CommunityDetection.find_communities_infomap,
    #         'louvain': CommunityDetection.find_communities_louvain
    #     }
    #
    #     assert method in possible_methods.keys()
    #
    #     f_method = possible_methods[method]
    #
    #     return f_method(graph)

# dict_communities = {}
#      # list_nodes = list(infomapWrapper.iterTree())
#      for node in infomapWrapper.iterTree():
#          # node = list_nodes[0]
#          # assert isinstance(node, infomap.InfomapIterator)
#          parent = node.parent
#          parent_index = None
#          if parent:
#              # assert isinstance(parent, infomap.InfoNode)
#              parent_index = node.parent.index
#          dict_communities[node.physicalId] = (parent_index, node.moduleIndex())
#          # parent.

#
# import community
# import networkx as nx
# import matplotlib.pyplot as plt
#
# #better with karate_graph() as defined in networkx example.
# #erdos renyi don't have true community structure
# G = nx.erdos_renyi_graph(30, 0.05)
#
# #first compute the best partition
# partition = community.best_partition(G)
#
# CommunityDetection.find_communities_louvain(G)
