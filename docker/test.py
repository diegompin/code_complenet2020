__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.documents_mhs import *

from pymongo import MongoClient
c = MongoClient(host='mongo', username='user', password='pwd', port=27017, authSource='mhs')


disconnect()


ConfigDocument.objects()

client = MongoClient('localhost',
    27017)

list(client.mhs.CFG_CONFIG.find())