__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.documents_mhs import HospitalDischargeDocument, NetworkHospitalDischargeDocument, \
    MetricsNetworkSharedCareAreaDocument, MetricsSharedCareAreaDocument
from mhs.src.library.network.community_detection import CommunityDetection
from mhs.src.shared_care_areas.networks.network_shared_care import NetworkSharedCare
from mhs.src.library.network.network_metric import NetworkMetricFactory
from mhs.src.shared_care_areas.metrics.metrics import MetricsSharedCareAreaFactory
from mhs.src.dao.base_dao import BaseDAO
import pandas as pd
import networkx as nx
from decimal import Decimal
import locale

locale.setlocale(locale.LC_ALL, '')


def formatter_date(x):
    return x.strftime('%b-%d')


def formatter_number(x):
    return "$%s$" % locale.format_string("%d", x, grouping=True)


def formatter_decimal(x, n=2):
    return '%s' % str.format(f'$%.{n}f$' % x)


def formatter_cientific(x):
    return '%.2E' % Decimal(x)


def get_network_latex():
    list_metrics = [
        NetworkMetricFactory.TYPE_NETWORK_NODES_TOTAL,
        NetworkMetricFactory.TYPE_NETWORK_LINKS_TOTAL,
        NetworkMetricFactory.TYPE_LINKS_WEIGHT_TOTAL,
        NetworkMetricFactory.TYPE_NETWORK_DENSITY_UNWEIGHTED,
        NetworkMetricFactory.TYPE_NODE_SHORTEST_PATH_WEIGHTED_AVERAGE,
        NetworkMetricFactory.TYPE_NODE_CLUSTERING_WEIGHTED_AVERAGE
    ]
    list_type_discharge = [HospitalDischargeDocument.TYPE_DISCHARGE_INPATIENT_FROM_ED,
                           HospitalDischargeDocument.TYPE_DISCHARGE_ED_ONLY]
    list_years = list([2012, 2013, 2014, 2015, 2016, 2017, 2018])
    method_hu = NetworkSharedCare.TYPE_HU
    list_network_metrics = MetricsNetworkSharedCareAreaDocument.objects(**{'method': method_hu})
    df = pd.DataFrame((i.get_data() for i in list_network_metrics))
    df_index = df.index == df.index
    # df_index = df_index & (df[MetricsNetworkSharedCareAreaDocument.type_discharge.name].isin(list_type_discharge))
    df_index = df_index & (df[MetricsNetworkSharedCareAreaDocument.year.name].isin(list_years))
    df_index = df_index & (df[MetricsNetworkSharedCareAreaDocument.metric.name].isin(list_metrics))
    df_latex = df.loc[df_index]
    df_latex = df_latex[[
        MetricsNetworkSharedCareAreaDocument.type_discharge.name,
        MetricsNetworkSharedCareAreaDocument.year.name,
        MetricsNetworkSharedCareAreaDocument.metric.name,
        MetricsNetworkSharedCareAreaDocument.metric_value.name
    ]]

    df_latex = pd.pivot_table(data=df_latex,
                              values=MetricsNetworkSharedCareAreaDocument.metric_value.name,
                              index=[
                                  MetricsNetworkSharedCareAreaDocument.type_discharge.name,
                                  MetricsNetworkSharedCareAreaDocument.year.name
                              ],
                              columns=MetricsNetworkSharedCareAreaDocument.metric.name)
    df_latex = df_latex.reset_index()
    df_latex = df_latex.rename(

        columns={
            MetricsNetworkSharedCareAreaDocument.type_discharge.name: 'Type of Discharge',
            MetricsNetworkSharedCareAreaDocument.year.name: 'Year',
            MetricsNetworkSharedCareAreaDocument.metric.name: 'Metric',
            MetricsNetworkSharedCareAreaDocument.metric_value.name: 'Value',
            NetworkMetricFactory.TYPE_NETWORK_NODES_TOTAL: '$n$',
            NetworkMetricFactory.TYPE_NETWORK_LINKS_TOTAL: '$m$',
            NetworkMetricFactory.TYPE_LINKS_WEIGHT_TOTAL: '$w$',
            NetworkMetricFactory.TYPE_NETWORK_DENSITY_UNWEIGHTED: '$\\rho$',
            NetworkMetricFactory.TYPE_NODE_SHORTEST_PATH_WEIGHTED_AVERAGE: '$l$',
            NetworkMetricFactory.TYPE_NODE_CLUSTERING_WEIGHTED_AVERAGE: '$c$'
        })

    df_latex = df_latex[[
        'Type of Discharge',
        'Year',
        '$n$',
        '$m$',
        '$w$',
        '$\\rho$',
        '$l$',
        '$c$'
    ]]

    df_latex = df_latex.set_index(['Type of Discharge', 'Year'])
    formatters = [formatter_cientific, formatter_cientific, formatter_cientific, formatter_cientific, formatter_decimal, formatter_cientific]
    latex = str(df_latex.to_latex(header=True, index=True, multirow=True, formatters=formatters, escape=False))
    return latex


def get_metrics_sca_latex():
    from mhs.src.shared_care_areas.analytics import AnalyticsSharedCareAreas

    mod_analytics = AnalyticsSharedCareAreas()

    col_metric = MetricsSharedCareAreaDocument.metric.db_field
    col_year = MetricsSharedCareAreaDocument.year.db_field
    col_type_discharge = MetricsSharedCareAreaDocument.type_discharge.db_field
    col_type_community_detection = MetricsSharedCareAreaDocument.type_community_detection.db_field

    method = NetworkSharedCare.TYPE_HU
    list_years = list([2012, 2018])

    list_sca_metric = [
        MetricsSharedCareAreaFactory.TYPE_NUMBER_COMMUNITIES,
        MetricsSharedCareAreaFactory.TYPE_LOCALIZATION_INDEX,
        MetricsSharedCareAreaFactory.TYPE_NUMBER_DISCHARGES,
        MetricsSharedCareAreaFactory.TYPE_CONDUCTANCE
    ]
    list_sca_type_discharge = [HospitalDischargeDocument.TYPE_DISCHARGE_INPATIENT_FROM_ED,
                               HospitalDischargeDocument.TYPE_DISCHARGE_ED_ONLY]
    # type_community_detection = list_type_community_detection_all,
    cols_groupby = [col_year, col_type_community_detection, col_metric, col_type_discharge]

    df_sca_metrics = mod_analytics.get_sca_metrics(cols_groupby,
                                                   method, list_sca_metric, None,
                                                   None,
                                                   None, list_years)
    df_sca_metrics = df_sca_metrics.groupby(
        ['MSA_SCA_TYPE_DISCHARGE', 'MSA_SCA_YEAR', 'MSA_SCA_TYPE_COMMUNITY_DETECTION', 'MSA_METRIC']).agg(
        {'value': 'mean'})
    df_sca_metrics = df_sca_metrics.reset_index()

    df_sca_metrics = pd.pivot_table(data=df_sca_metrics,
                                    values='value',
                                    index=[
                                        MetricsSharedCareAreaDocument.type_discharge.db_field,
                                        MetricsSharedCareAreaDocument.year.db_field,
                                        MetricsSharedCareAreaDocument.type_community_detection.db_field
                                    ],
                                    columns=MetricsSharedCareAreaDocument.metric.db_field
                                    )

    df_sca_metrics = df_sca_metrics.rename(columns=str).reset_index()

    df_sca_metrics = df_sca_metrics.rename_axis(None, axis=1)

    df_sca_metrics = df_sca_metrics.rename(

        columns={
            MetricsSharedCareAreaDocument.type_discharge.db_field: 'Type of Discharge',
            MetricsSharedCareAreaDocument.year.db_field: 'Year',
            MetricsSharedCareAreaDocument.type_community_detection.db_field: 'Community Detection',
            MetricsSharedCareAreaFactory.TYPE_NUMBER_COMMUNITIES: '$n_c$',
            MetricsSharedCareAreaFactory.TYPE_LOCALIZATION_INDEX: '$\langle li \rangle$',
            MetricsSharedCareAreaFactory.TYPE_CONDUCTANCE: '$\langle c \rangle$',
            MetricsSharedCareAreaFactory.TYPE_NUMBER_DISCHARGES: ' $\langle d \rangle$'

        })

    df_sca_metrics = df_sca_metrics.set_index(['Type of Discharge', 'Year', 'Community Detection'])
    formatters = [formatter_number, formatter_decimal, formatter_decimal, formatter_number]
    latex = str(df_sca_metrics.to_latex(header=True, index=True, multirow=True, formatters=formatters, escape=False))
    return latex


latex_metrics_network = get_network_latex()

print(latex_metrics_network)

latex_metrics_sca = get_metrics_sca_latex()

print(latex_metrics_sca)
# list_sca_metrics = MetricsSharedCareAreaDocument.objects(**{'method': method_hu})
#
# df_sca_metrics = pd.DataFrame((i.get_data() for i in list_sca_metrics))
#
# df_sca_metrics_index = df_sca_metrics.index == df_sca_metrics.index
#
# df_sca_metrics_index = df_sca_metrics_index & (df_sca_metrics[MetricsSharedCareAreaDocument.type_discharge.name].isin(list_type_discharge))
# df_sca_metrics_index = df_sca_metrics_index & (df_sca_metrics[MetricsSharedCareAreaDocument.year.name].isin(list_years))
#
# list_sca_metrics = [
#     MetricsSharedCareAreaFactory.TYPE_NUMBER_COMMUNITIES,
#     MetricsSharedCareAreaFactory.TYPE_LOCALIZATION_INDEX,
#     MetricsSharedCareAreaFactory.TYPE_CONDUCTANCE,
#     MetricsSharedCareAreaFactory.TYPE_NUMBER_DISCHARGES
#
# ]
# df_sca_metrics_index = df_sca_metrics_index & (df_sca_metrics[MetricsSharedCareAreaDocument.metric.name].isin(list_sca_metrics ))
# df_sca_metrics_latex = df_sca_metrics.loc[df_sca_metrics_index]
#
# df_latex = df_latex[[
#     MetricsSharedCareAreaDocument.type_discharge.name,
#     MetricsSharedCareAreaDocument.year.name,
#     MetricsSharedCareAreaDocument.
#
# ]]
#
