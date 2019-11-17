__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mongoengine.fields import GeoJsonBaseField
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely.geometry.base import BaseGeometry
import numpy as np
#
from mongoengine.errors import (ValidationError, )


# # mp = df['geometry'].values[1]


class DynamicGeoJsonBaseField(GeoJsonBaseField):
    __TYPES_IMPLEMENTED__ = ['Polygon', 'MultiPolygon']

    def validate(self, value):
        if not isinstance(value, BaseGeometry):
            self.error(f'{self.name} is not a BaseGeometry')

        # TODO Implement BaseGeometry.GEOMETRY_TYPES
        if value.type not in self.__TYPES_IMPLEMENTED__:
            self.error(
                f'{value.type} is not implemented. Currently the only types implemented are '
                f'{list(self.__TYPES_IMPLEMENTED__)}')

    def to_mongo(self, value: BaseGeometry):
        dict_mongo_geo_json = dict()
        dict_mongo_geo_json['type'] = value.type
        coordinates = []
        if isinstance(value, Polygon):
            coordinates = list(value.exterior.coords)
        elif isinstance(value, MultiPolygon):
            coordinates = [list(i.exterior.coords) for i in list(value)]
        dict_mongo_geo_json['coordinates'] = coordinates
        return super().to_mongo(dict_mongo_geo_json)

    def to_python(self, value):
        type = value['type']
        if type == 'Polygon':
            return Polygon(value['coordinates'])
        elif type == 'MultiPolygon':
            [Polygon(i) for i in value['coordinates']]
            return MultiPolygon([Polygon(i) for i in value['coordinates']])
        else:
            raise Exception(f'Type {type} not supported.')
