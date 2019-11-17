import pika
import logging
import graph_tool as gt
import json
import numpy as np
from graph_tool import inference
import sys
import time
logging.basicConfig()

def findCommunities(filename):
    trials = 1
    fullFile =f'{filename}.graphml'
    print(fullFile)
    graph = gt.load_graph(fullFile)
    lowest_entropy = np.inf
    best_community = None
    for i in range(trials):
        state = inference.minimize_blockmodel_dl(graph, deg_corr=True,verbose=True)
        b = state.get_blocks()
        print(state.entropy())
        if state.entropy() < lowest_entropy:
            best_community = b
            lowest_entropy = state.entropy()
    communityMapping = dict()
    nodeList = list()
    communityID = list()
    for v in graph.vertices():
        nodeList.append(str(graph.vertex_properties["_graphml_vertex_id"][v]))
        communityID.append(str(best_community[v]))
    communityMapping['NODE_ID'] = nodeList
    communityMapping['COMMUNITY_ID'] = communityID
    return communityMapping
def on_request(ch, method, props, body):
    communityMapping = findCommunities(body.decode("utf-8"))
    response = json.dumps(communityMapping)
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)

while True:
    try:
        credentials = pika.PlainCredentials(username='user', password='bitnami')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq', credentials=credentials, heartbeat=65535,blocked_connection_timeout=3600))
        channel = connection.channel()
        channel.queue_declare(queue='StochasticBlockModel')
        channel.basic_consume(queue='StochasticBlockModel', on_message_callback=on_request)
        print(" [x] Awaiting RPC requests")
        channel.start_consuming()
    except:
        print("Unexpected error:", sys.exc_info()[0])
        time.sleep(5)
