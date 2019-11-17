__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

import geopandas as geo
from mhs.src.dao.mhs.documents_mhs import *
# from mhs.src.dao.base_dao import DAOMongo
from mhs.src.dao.base_dao import BaseDAO
import pandas as pd
from shapely.geometry.point import Point
from shapely.geometry import mapping, shape
import matplotlib.pyplot as plt
import numpy as np
from mhs.config import MHSConfigManager
from mhs.src.library.network.community_detection import CommunityDetection
from mhs.src.library.file.utils import check_folder
from shapely.geometry import mapping, shape
import itertools as ite


class PlotSCA(object):

    def __init__(self):
        # self.dao = DAOMongo()
        # self.__dao__sca__ = BaseDAO(SharedCareArea)
        # self.__dao_zcta_county__ = BaseDAO(ZctaCountyDocument)
        # self.__dao_shapefile__ = BaseDAO(ShapefileDocument)
        # dict_match_zcta = {
        #     ZctaCountyDocument.geocode_fips_state: '06'
        # }
        # self.df_zcta_ca = pd.DataFrame(self.__dao_zcta_county__.obtain_pipeline(dict_match=dict_match_zcta))
        self.df_zcta_ca = pd.DataFrame(
            [i.get_data() for i in ZctaCountyDocument.objects(**{ZctaCountyDocument.geocode_fips_state.name: '06'})])
        self.df_shapefile_ca = pd.DataFrame([i.get_data() for i in ShapefileDocument.objects()])

        self.df_shapefile_ca = self.df_shapefile_ca.loc[
            self.df_shapefile_ca[ShapefileDocument.geocode_fips.name].isin(
                self.df_zcta_ca[ZctaCountyDocument.geocode_fips.name])]

        # self.conf = MHSConfigManager()
        # self.path_plots = self.conf.plots

    def plot(self, method, type_discharge, type_community_detection, year, path_plots=None):
        plt.style.use('classic')

        # def get_cm(N):
        #     cmap = plt.cm.get_cmap('viridis')
        #     colors = cmap([int(i) for i in np.linspace(0, cmap.N, N)])
        #
        #     from matplotlib.colors import ListedColormap
        #     # cmap = ListedColormap(colors, name='Sequential')
        #     cmap = ListedColormap(colors, name='my', N=N)
        #     return cmap

        crs = {'init': 'epsg:2163'}

        fig, ax = plt.subplots(1, figsize=(20, 20))
        # ax = fig.add_subplot(111)
        ax.axis('off')

        # dict_match_sca = {
        #     SharedCareArea.method: method,
        #     SharedCareArea.type_discharge: type_discharge,
        #     SharedCareArea.type_community_detection: type_community_detection,
        #     SharedCareArea.year: year
        #
        # }
        sca_objs = SharedCareArea.objects(**{
            SharedCareArea.method.name: method,
            SharedCareArea.type_discharge.name: type_discharge,
            SharedCareArea.type_community_detection.name: type_community_detection,
            SharedCareArea.year.name: year
        })
        df_sca = pd.DataFrame([i.get_data() for i in sca_objs])
        df_sca[SharedCareArea.sca_id.name] = df_sca[SharedCareArea.sca_id.name].apply(lambda x: int(x))

        geo_ca = geo.GeoDataFrame(self.df_shapefile_ca, crs=crs)
        geo_ca = geo_ca.rename(columns={ShapefileDocument.geocode_geometry.name: 'geometry'})

        geo_ca_sca = geo_ca.merge(df_sca, left_on=ShapefileDocument.geocode_fips.name,
                                  right_on=SharedCareArea.zcta.name)

        # geo_ca.plot(linewidth=0, ax=ax, edgecolor='0', color='grey')

        N = len(geo_ca_sca[SharedCareArea.sca_id.name].unique())
        # cmap = get_cm(N)

        # geo_ca_sca.plot(column=SharedCareArea.sca_id.name, linewidth=1, ax=ax, edgecolor='0', color='white')


        df_shapefile_annotation = geo_ca_sca.dissolve(by=SharedCareArea.sca_id.name)
        df_shapefile_annotation = df_shapefile_annotation.reset_index()

        def max_list(l, k):
            n = len(l)
            p = int(n / k)
            i = 0
            l_prime = []
            while (i < n):
                m = i * p % n
                l_prime.append(l[m])
                i += 1
            return l_prime

        df_shapefile_annotation[SharedCareArea.sca_id.name] = max_list(df_shapefile_annotation[SharedCareArea.sca_id.name], k=9)

        # df_shapefile_annotation = df_shapefile_annotation.sort_values(by=SharedCareArea.sca_id.name)
        df_shapefile_annotation.plot(column=SharedCareArea.sca_id.name, linewidth=1, ax=ax, edgecolor='grey', cmap=plt.cm.get_cmap('Pastel1'))
        for k, v in zip(df_shapefile_annotation.centroid.index, df_shapefile_annotation.centroid.values):
            plt.annotate(s=k, xy=v.coords[0],
                         horizontalalignment='center', size=20, color='black')
        # path_plots = self.path_plots

        if not path_plots:
            path_plots = '.'
        folder = f'{path_plots}'
        check_folder(folder)
        filename = '_'.join([str(s) for s in
                             [method, type_discharge, type_community_detection, year]])
        # plt.tight_layout()
        plt.savefig(f'{folder}/map_{filename}.pdf', transparent=True)
        plt.close()

#
# self = PlotSCA()
#
# method = 'HU'
#
# list_type_discharge = [HospitalDischargeDocument.TYPE_DISCHARGE_INPATIENT_FROM_ED, HospitalDischargeDocument.TYPE_DISCHARGE_ED_ONLY]
# list_type_community_detection_all = list(CommunityDetection.get_types())
# list_years = list([2018])
#
#
# combination = list(ite.product(list_type_discharge, list_years, list_type_community_detection_all ))
# for (type_discharge, year, type_community_detection) in combination:
#     # metric = MetricsSharedCareArea.TYPE_LOCALIZATION_INDEX
#     # normalized = False
#     # type_community_detection = CommunityDetection.TYPE_INFOMAP
#     # type_discharge = HospitalDischargeDocument.TYPE_DISCHARGE_INPANTIENT
#
#     self.plot(method=method,
#               type_discharge=type_discharge,
#               type_community_detection=type_community_detection,
#               year=year)
#

#
#
#
#
# crs = {'init': 'epsg:2163'}
#
# fig, ax = plt.subplots(1, figsize=(20, 20))
# # ax = fig.add_subplot(111)
# ax.axis('off')
#
# dict_match_sca = {
#     SharedCareArea.method: method,
#     SharedCareArea.type_discharge: type_discharge,
#     SharedCareArea.type_community_detection: type_community_detection,
#     SharedCareArea.year: year
#
# }
# df_sca = pd.DataFrame(self.__dao__sca__.obtain_pipeline(dict_match=dict_match_sca))
# geo_ca = geo.GeoDataFrame(self.df_shapefile_ca, crs=crs)
# geo_ca = geo_ca.rename(columns={ShapefileDocument.geocode_geometry.db_field: 'geometry'})
#
# geo_ca_sca = geo_ca.merge(df_sca, left_on=ShapefileDocument.geocode_fips.db_field,
#                                   right_on=SharedCareArea.zcta.db_field)
#
# geo_ca.plot(linewidth=2, ax=ax, edgecolor='0', color='grey')
# N = len(geo_ca_sca[SharedCareArea.sca_id.db_field].unique())
# # cmap = get_cm(N)
#
# geo_ca_sca.plot(column=SharedCareArea.sca_id.db_field, linewidth=0, ax=ax, edgecolor='0')
#
# # df_shapefile_annotation = geo_ca_sca.dissolve(by=SharedCareArea.sca_id.db_field).centroid
# # for k, v in zip(df_shapefile_annotation.index, df_shapefile_annotation.values):
# #     plt.annotate(s=k, xy=v.coords[0],
# #                  horizontalalignment='center', size=20, color='grey')
#
# # [v.coords[0] for v in geo_facility_hf['geometry']]
# # for  k,v,z in zip(geo_facility_hf['facility_name'],  geo_facility_hf['geometry'], geo_facility_hf['count']):
# #     plt.annotate(s=k, xy=v.coords[0], horizontalalignment='center', size=1)
# # plt.legend()
#
# path_plots = self.path_plots
# folder = f'{path_plots}/complenet2020/maps'
# check_folder(folder)
# # filename = f'{cols_groupby[0]}_{metric}_{normalized}_{method}_{type_community_detection}_{type_discharge}_{year}'
# filename = '_'.join([str(s) for s in
#                      [method, type_discharge, type_community_detection, year]])
# # plt.tight_layout()
# plt.savefig(f'{folder}/map_{filename}.pdf')
# plt.close()
#


# shfi = geo.read_file('/Volumes/MHS/data_acquisition/CENSUS/SHAPEFILE/CENSUS_SHAPEFILE_ZCTA.shp')

