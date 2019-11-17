from mhs.src.library.file.utils import check_folder

__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.config import MHSConfigManager
import pandas as pd
from mhs.src.shared_care_areas.networks.network_shared_care import NetworkSharedCare
from mhs.src.library.network.community_detection import CommunityDetection
from mhs.src.shared_care_areas.metrics.metrics import MetricsSharedCareAreaFactory
from mhs.src.dao.mhs.documents_mhs import MetricsSharedCareAreaDocument, HospitalDischargeDocument
from mhs.src.dao.base_dao import BaseDAO
import itertools as ite

import bootstrapped.bootstrap as bs
import bootstrapped.stats_functions as bs_stats
import seaborn as sns
import matplotlib.pyplot as plt

import numpy as np

from mhs.src.shared_care_areas.analytics import AnalyticsSharedCareAreas


class PlotSharedCareArea(object):

    def __init__(self):

        self.analytics = AnalyticsSharedCareAreas()
        # self.conf = MHSConfigManager()
        # self.path_plots = conf.plots

    def plot(sefl, metric=None,
             normalized=None,
             method=None,
             type_community_detection=None,
             type_discharge=None,
             year=None,
             cols_groupby=None,
             path_plots=None,
             sharex=True,
             sharey=True):

        # fig, ax = plt.subplots(1)

        # df = self.df

        df_li_boot_melted = self.analytics.get_sca_metrics(cols_groupby, method, metric, normalized,
                                                           type_community_detection,
                                                           type_discharge, year)

        # df_metric_grouped

        # sns.violinplot(data=df_li_boot_melted, x=cols_groupby[0], y='value',
        #                scale_hue=True,
        #                hue=cols_groupby[1],
        #                points=0,
        #                # inner="points",
        #                palette="Pastel1", figsize=(20, 35))

        # df_li_boot_melted = df_li_boot_melted.sort_values(by=cols_groupby)

        cycle_colors = ite.cycle(sns.color_palette('colorblind'))
        list_colors = [(CommunityDetection.TYPES[i], next(cycle_colors)) for i in
                       range(len(CommunityDetection.get_types()))]
        list_colors = dict(list_colors)

        list_colors = [list_colors[i] for i in type_community_detection]
        # list_colors
        col = None
        row = None

        if len(cols_groupby) > 2:
            col = cols_groupby[2]

        if len(cols_groupby) > 3:
            row = cols_groupby[3]

        g = sns.catplot(data=df_li_boot_melted,
                        x=cols_groupby[0],
                        y='value',
                        # scale_hue=True,
                        hue=cols_groupby[1],
                        # points=0,
                        kind='point',
                        dodge=True,
                        # color=list_colors,
                        # n_boot=100,
                        ci='sd',
                        # inner="points",
                        palette=list_colors,
                        # figsize=(20, 35),
                        col=col,
                        row=row,
                        sharex=sharex,
                        sharey=sharey,
                        legend=False,
                        legend_out=False,
                        )

        g.set_titles(template='{col_name}, {row_name}')
        g.set_ylabels('Metric Value')

        # g.set_axis_labels('', "{col_name}")

        # plt.xlabel("Characteristics")
        # plt.ylabel(metric)
        # ax.set_ylabel(metric)
        # for tick in ax.get_xticklabels():
        #     tick.set_rotation(20)
        # plt.xticks(rotation=5)
        # plt.xlabel(cols_groupby[0])
        # plt.ylim([-3, 3])
        if not path_plots:
            path_plots = '.'
        # path_plots = self.path_plots

        check_folder(path_plots)
        filename = '_'.join([str(s) for s in
                             [cols_groupby[0], metric, normalized, method, type_community_detection, type_discharge,
                              year]])

        if len(cols_groupby) > 2:
            filename = f'{cols_groupby[2]}_{filename}'

        if len(cols_groupby) > 3:
            filename = f'{cols_groupby[3]}_{filename}'
        plt.tight_layout()

        plt.savefig(f'{path_plots}/{filename}.pdf', transparent=True)

        plt.close()


# tips = sns.load_dataset("tips")


col_metric = MetricsSharedCareAreaDocument.metric.db_field
col_method = MetricsSharedCareAreaDocument.method.db_field
col_year = MetricsSharedCareAreaDocument.year.db_field
col_type_discharge = MetricsSharedCareAreaDocument.type_discharge.db_field
col_type_community_detection = MetricsSharedCareAreaDocument.type_community_detection.db_field
list_metrics = MetricsSharedCareAreaFactory.TYPES

# list_type_community_detection = CommunityDetection.TYPES

# list_methods = [NetworkSharedCare.TYPE_HU]

list_type_discharge = HospitalDischargeDocument.TYPES_DISCHARGE
list_type_community_detection_all = list(CommunityDetection.get_types())
# list_type_community_detection_without_lp = list(CommunityDetection.get_types())
# list_type_community_detection_without_lp.remove(CommunityDetection.TYPE_LABELPROPAGATION)
list_years = list([2012, 2013, 2014, 2015, 2016, 2017, 2018])

list_normalized = [False]

self = PlotSharedCareArea()

method_hu = NetworkSharedCare.TYPE_HU
# conf = MHSConfigManager()
# path_plots = conf.plots

path_plots = '/Volumes/MHS/data_curation/SHARED_CARE_AREA/COMMUNITY_PLOTS'

self.plot(
    method=method_hu,
    metric=[MetricsSharedCareAreaFactory.TYPE_NUMBER_COMMUNITIES,
            MetricsSharedCareAreaFactory.TYPE_LOCALIZATION_INDEX,
            MetricsSharedCareAreaFactory.TYPE_NUMBER_DISCHARGES,
            MetricsSharedCareAreaFactory.TYPE_CONDUCTANCE],
    type_discharge=[HospitalDischargeDocument.TYPE_DISCHARGE_INPATIENT_FROM_ED,
                    HospitalDischargeDocument.TYPE_DISCHARGE_ED_ONLY],
    type_community_detection=list_type_community_detection_all,
    cols_groupby=[col_year, col_type_community_detection, col_metric, col_type_discharge],
    path_plots=path_plots, sharex=False, sharey=False)

# combination = list(ite.product(list_metrics, list_type_discharge))
# for (metric, type_discharge) in combination:
#     # metric = MetricsSharedCareArea.TYPE_LOCALIZATION_INDEX
#     # normalized = False
#     # type_community_detection = CommunityDetection.TYPE_INFOMAP
#     # type_discharge = HospitalDischargeDocument.TYPE_DISCHARGE_INPANTIENT
#
#     self.plot(
#         method=method_hu,
#         metric=metric,
#         year=list_years,
#         type_community_detection=list_type_community_detection_all,
#         type_discharge=type_discharge,
#         cols_groupby=[col_year, col_type_community_detection],
#         path_plots=path_plots)
#     #
#     # self.plot(
#     #     method=method_hu,
#     #     metric=metric,
#     #     year=list_years,
#     #     type_community_detection=list_type_community_detection_without_lp,
#     #     type_discharge=type_discharge,
#     #     cols_groupby=[col_year, col_type_community_detection])
#
# combination = list(ite.product(list_metrics, list_years))
# for (metric, year) in combination:
#     # metric = MetricsSharedCareArea.TYPE_LOCALIZATION_INDEX
#     # normalized = False
#     # type_community_detection = CommunityDetection.TYPE_INFOMAP
#     # type_discharge = HospitalDischargeDocument.TYPE_DISCHARGE_INPANTIENT
#
#     self.plot(
#         method=method_hu,
#         metric=metric,
#         year=year,
#         type_community_detection=list_type_community_detection_all,
#         cols_groupby=[col_type_discharge, col_type_community_detection],
#         path_plots=path_plots)


# self.plot(
#     method=method_hu,
#     metric=metric,
#     year=year,
#     type_community_detection=list_type_community_detection_without_lp,
#     cols_groupby=[col_type_discharge, col_type_community_detection])

#
# from itertools import cycle
#
#
# colors = ['g', 'y', 'r', 'b', 'k', 'm', 'c']
# colors = cycle(colors)
# par_linewidth = 3
# par_alpha = 1
# label = 'Label'
#
# fig, ax = plt.subplots()
# fig.subplots_adjust(right=0.75)
#
#
# # ax.errorbar(x=[0],
# #                 y=[1],
# #                 yerr=([0], [2]),
# #                 fmt='o',
# #                 color=next(colors),
# #                 linewidth=par_linewidth,
# #                 # linestyle=':',
# #                 # mew=1,
# #                 ms=7,
# #                 alpha=par_alpha,
# #                 label=label,
# #                 capthick=2,
# #                 capsize=10
# #                 )
# x1 = ax.twinx()
# x1.set_ylabel = 'A'
# x1.errorbar(x=[0],
#                 y=[.5],
#                 yerr=(1),
#                 fmt='o',
#                 color=next(colors),
#                 linewidth=par_linewidth,
#                 # linestyle=':',
#                 # mew=1,
#                 ms=7,
#                 alpha=par_alpha,
#                 label='a',
#                 capthick=2,
#                 capsize=10
#                 )
# x1.tick_params(axis='y',labelsize=16)
# x2 = ax.twinx()
# x2.set_ylabel = 'B'
# x2.errorbar(x=[1],
#                 y=[.5],
#                 yerr=(1),
#                 fmt='o',
#                 color=next(colors),
#                 linewidth=par_linewidth,
#                 # linestyle=':',
#                 # mew=1,
#                 ms=7,
#                 alpha=par_alpha,
#                 label='b',
#                 capthick=2,
#                 capsize=10
#                 )
#
# x2.spines["right"].set_position(("axes", 1.2))
# # x2.set_ylim(x1.get_ylim()[0]*12, x1.get_ylim()[1]*12)
# plt.legend(fontsize=14, loc=6)
#
# ax.yaxis.set_visible(False)
# plt.savefig('t.pdf')
# plt.close()

# combination = list(ite.product(list_metrics, list_normalized, list_type_discharge))
#
# for (metric, normalized, type_discharge) in combination:
#     # metric = MetricsSharedCareArea.TYPE_LOCALIZATION_INDEX
#     # normalized = False
#     # type_community_detection = CommunityDetection.TYPE_INFOMAP
#     # type_discharge = HospitalDischargeDocument.TYPE_DISCHARGE_INPANTIENT
#
#     self.plot(metric=metric,
#               method=method_hu,
#               normalized=normalized,
#               # type_community_detection=type_community_detection,
#               type_discharge=type_discharge,
#               cols_groupby=[col_year, col_type_community_detection])
#
# combination = list(ite.product(list_metrics, list_normalized, list_years))
#
# for (metric, normalized, year) in combination:
#     # metric = MetricsSharedCareArea.TYPE_LOCALIZATION_INDEX
#     # normalized = False
#     # type_community_detection = CommunityDetection.TYPE_INFOMAP
#     # type_discharge = HospitalDischargeDocument.TYPE_DISCHARGE_INPANTIENT
#
#     self.plot(metric=metric,
#               method=method_hu,
#               normalized=normalized,
#               # type_community_detection=type_community_detection,
#               year=year,
#               cols_groupby=[col_type_discharge, col_type_community_detection])
#
#

# df_li_boot_melted.columns
# year = 2016

#
# ax = plt.subplot()
# ax.hist(df_li_boot.loc[df_li_boot[col_year] == year, col_metric_value].values[0], label='UNIPARTITE', histtype='step')
# ax.hist(df_li_boot.loc[df_li_boot[col_year] == year, col_metric_value].values[1], label='BIPARTITE (PINHEIRO)',
#         histtype='step')
# ax.hist(df_li_boot.loc[df_li_boot[col_year] == year, col_metric_value].values[2], label='BIPARTITE (YILDIRIM)',
#         histtype='step')
# ax.legend()


# import matplotlib.pyplot as plt

#
# folder = '/Volumes/MHS/data_curation/SHARED_CARE_AREA/04_CALCULATE_METRICS'
# filename = f'{method}_{year}_{discharge_type}_{type_community_detection}_{metric}.csv'
# filepath = f'{folder}/{filename}'
# df = pd.read_csv(filepath)
# print(df.mean())


# MetricsSharedCareAreaDocument.metric

# class PlotSharedCareArea(object):
#
#     def get_airflow_id(airflow_id: str, **kwargs):
#         id = '_'.join([f'{str(i)}' for i in kwargs.values()])
#         return get_filename(f'{id}')
#
#     def get_operator_folder_path(self, name):
#         config = MHSConfigManager()
#         folder_root = config.data_curation
#         folder_class = 'SHARED_CARE_AREA'
#         folder_name = name
#         folder_path = f'{folder_root}/{folder_class}/{folder_name}'
#         check_folder(folder_path)
#         return folder_path
#
#
#     def plot(self):
#
#
#         method = NetworkSharedCare.TYPE_HU
#         discharge_type = HospitalDischargeDocument.TYPE_DISCHARGE_INPANTIENT
#         type_community_detection = CommunityDetection.TYPE_INFOMAP
#         metric = MetricsSharedCareArea.TYPE_LOCALIZATION_INDEX
#
#
#
#
#         kwargs = {
#             method:method,
#             discharge_type: discharge_type,
#             type_community_detection: type_community_detection,
#             metric: metric
#         }
#         filename = get_airflow_id('', **kwargs)
#
#
#         folder_path_in = self.get_operator_folder_path(SharedCareAreaScript.TYPE_01_CREATE_NETWORK)
#                 filename_in = f'{method}_{year}_{discharge_type}'
#                 filepath_in = f'{folder_path_in}/{filename_in}'
#
#         df = pd.read_csv(f'{filename}.csv')
