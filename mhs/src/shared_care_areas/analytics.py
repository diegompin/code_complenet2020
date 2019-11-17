__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'
from mhs.src.dao.mhs.documents_mhs import MetricsSharedCareAreaDocument, HospitalDischargeDocument
from mhs.src.shared_care_areas.metrics.metrics import MetricsSharedCareAreaFactory
import pandas as pd
import bootstrapped.bootstrap as bs
import bootstrapped.stats_functions as bs_stats
from mhs.src.dao.base_dao import BaseDAO


class AnalyticsSharedCareAreas(object):

    def __init__(self):
        mod = BaseDAO(MetricsSharedCareAreaDocument)
        self.df = pd.DataFrame.from_dict(mod.obtain_pipeline(dict_match={}, math_none=True))

    def get_sca_metrics(self, cols_groupby, method, metric, normalized, type_community_detection, type_discharge,
                        year):
        col_metric_value = MetricsSharedCareAreaDocument.metric_value.db_field
        df_metric = self.get_df_metrics(method, metric, normalized, type_community_detection,
                                        type_discharge, year)
        df_metric_grouped = df_metric
        if cols_groupby:
            df_metric_grouped = df_metric.groupby(cols_groupby)
        boostraps = 1000

        #
        def f_bootstrap(values):
            return bs.bootstrap(values, stat_func=bs_stats.mean, num_iterations=boostraps, return_distribution=True)

        #
        df_metric_boots = df_metric_grouped.apply(lambda x: pd.Series({
            col_metric_value:
                f_bootstrap(x[col_metric_value].values)}
        ))
        df_metric_boots = df_metric_boots.reset_index()
        df_metric_boots = df_metric_boots.dropna()
        # df_metric_boots = df_metric_boots.sort_values(by=[])
        df_metric_boot_values = df_metric_boots[col_metric_value].apply(lambda x: pd.Series(x))
        del df_metric_boots[col_metric_value]
        df_metric_boot_values = pd.concat([df_metric_boots, df_metric_boot_values], axis=1)
        # df_li_boot_melted = pd.melt(df_metric_grouped, value_vars=col_metric_value, id_vars=cols_groupby)
        df_li_boot_melted = pd.melt(df_metric_boot_values, value_vars=range(0, boostraps), id_vars=cols_groupby)
        return df_li_boot_melted

    def get_df_metrics(self, method, metric, normalized, type_community_detection, type_discharge, year):
        df = self.df
        df_filter = df.index == df.index
        col_metric = MetricsSharedCareAreaDocument.metric.db_field
        col_normalized = MetricsSharedCareAreaDocument.normalized.db_field
        col_method = MetricsSharedCareAreaDocument.method.db_field
        col_type_community_detection = MetricsSharedCareAreaDocument.type_community_detection.db_field
        col_type_discharge = MetricsSharedCareAreaDocument.type_discharge.db_field
        col_year = MetricsSharedCareAreaDocument.year.db_field

        #
        # dict_type_community_detection = {CommunityDetection.TYPE_INFOMAP: 0,
        #                                  CommunityDetection.TYPE_BLOCKMODEL: 1,
        #                                  CommunityDetection.TYPE_LOUVAIN: 2,
        #                                  CommunityDetection.TYPE_LABELPROPAGATION: 3}
        #
        # df[col_type_community_detection] = pd.Categorical(df[col_type_community_detection],
        #                                                   categories=sorted(
        #                                                       dict_type_community_detection,
        #                                                       key=dict_type_community_detection.get),
        #                                                   ordered=True)

        if metric is not None:
            if not isinstance(metric, list):
                metric = [metric]

            # df_filter = df_filter & (df[col_metric] == metric)
            df_filter = df_filter & (df[col_metric].isin(metric))

        if normalized is not None:
            df_filter = df_filter & (df[col_normalized] == normalized)
        if method is not None:
            df_filter = df_filter & (df[col_method] == method)
        if type_community_detection is not None:
            if not isinstance(type_community_detection, list):
                type_community_detection = [type_community_detection]
            df_filter = df_filter & (df[col_type_community_detection].isin(type_community_detection))
        if type_discharge is not None:
            if not isinstance(type_discharge, list):
                type_discharge = [type_discharge]
            df_filter = df_filter & (df[col_type_discharge].isin(type_discharge))
        if year is not None:
            if not isinstance(year, list):
                year = [year]
            df_filter = df_filter & (df[col_year].isin(year))
        df_metric = df[df_filter]

        # df_metric[col_year] = df_metric[col_year].apply(lambda x: str(int(x)) if np.isfinite(x) else 'ALL')

        # df_metric.loc[pd.isnull(df_metric['MSA_METRIC_VALUE'])]
        # df_metric[col_metric] = df_metric[col_metric].astype(str)
        # df_metric[col_type_discharge] = df_metric[col_type_discharge].astype(str)

        dict_type_metric = {
            MetricsSharedCareAreaFactory.TYPE_NUMBER_COMMUNITIES: 0,
            MetricsSharedCareAreaFactory.TYPE_LOCALIZATION_INDEX: 1,
            MetricsSharedCareAreaFactory.TYPE_CONDUCTANCE: 2,
            MetricsSharedCareAreaFactory.TYPE_NUMBER_DISCHARGES: 3,

        }
        dict_type_metric = dict([i for i in dict_type_metric.items() if i[0] in df_metric[col_metric].unique()])

        df_metric[col_metric] = pd.Categorical(df_metric[col_metric],
                                               categories=sorted(dict_type_metric, key=dict_type_metric.get),
                                               ordered=True)

        dict_type_discharge = {HospitalDischargeDocument.TYPE_DISCHARGE_INPATIENT_FROM_ED: 0,
                               HospitalDischargeDocument.TYPE_DISCHARGE_INPANTIENT: 1,
                               HospitalDischargeDocument.TYPE_DISCHARGE_AS_ONLY: 2,
                               HospitalDischargeDocument.TYPE_DISCHARGE_ED_ONLY: 3}
        dict_type_discharge = dict(
            [i for i in dict_type_discharge.items() if i[0] in df_metric[col_type_discharge].unique()])
        df_metric[col_type_discharge] = pd.Categorical(df_metric[col_type_discharge],
                                                       categories=sorted(dict_type_discharge,
                                                                         key=dict_type_discharge.get),
                                                       ordered=True)
        df_metric = df_metric.sort_values(by=[col_method,
                                              col_metric,
                                              col_type_discharge,
                                              col_type_community_detection,
                                              col_year])

        return df_metric
