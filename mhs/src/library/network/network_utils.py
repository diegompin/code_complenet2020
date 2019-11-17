__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

import os
from mhs.config import MHSConfigManager
import networkx as nx


def build_from_dataframe(df):
    col_source = df.columns[0]
    col_target = df.columns[1]
    col_weight = df.columns[2]
    weights = [float(f) for f in df[col_weight]]
    edges = list(zip(df[col_source], df[col_target], weights))
    G = nx.Graph()
    G.add_weighted_edges_from(edges)
    # G = nx.from_edgelist(edges)
    G.edges(data=True)
    return G


def check_folder(folder):
    if not os.path.isdir(folder):
        os.makedirs(folder)


def write_edgelist(G, folder, filename, weighted=True):
    with open(f'{folder}/{filename}-edgeList.txt','w+') as edgeFile:
        for edge in G.edges(data='weight'):
            if weighted:
                edgeFile.write(f'{edge[0]} {edge[1]} {edge[2]}\n')
            else:
                edgeFile.write(f'{edge[0]} {edge[1]}\n')


def write_graph(G, folder, filename):
    check_folder(folder)
    nx.write_graphml(G, path=f'{folder}/{filename}.graphml')
    nx.write_pajek(G, path=f'{folder}/{filename}.pajek')
    write_edgelist(G, folder, filename)

