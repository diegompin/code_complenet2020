__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'
import pandas as pd

from mhs.src.dao.base_dao import BaseDAO
from mhs.src.shared_care_areas.metrics.base import MetricsSharedCareAreaFactory
from mhs.src.dao.mhs.documents_mhs import MetricsSharedCareAreaDocument
import statsmodels.api as sm
from statsmodels.formula.api import ols
import statsmodels.formula.api as smf
import bootstrapped.bootstrap as bs
import bootstrapped.stats_functions as bs_stats

mod = BaseDAO(MetricsSharedCareAreaDocument)
df = pd.DataFrame.from_dict(mod.obtain_pipeline(dict_match={}, math_none=True))
df_metric = df[
    (df['MSA_METRIC'] == MetricsSharedCareAreaFactory.TYPE_LOCALIZATION_INDEX)
    & (df['MSA_SCA_NORMALIZED'] == False)
    & (pd.notnull(df['MSA_SCA_YEAR']))
    & (df['MSA_SCA_TYPE_COMMUNITY_DETECTION'] == 'INFOMAP')
    & (df['MSA_SCA_TYPE_DISCHARGE'] == 'ED Only')
]
cols = [
    'MSA_SCA_METHOD',
    'MSA_SCA_TYPE_DISCHARGE',
    'MSA_SCA_TYPE_COMMUNITY_DETECTION',
    'MSA_SCA_YEAR'
]
col_metric_value = 'MSA_METRIC_VALUE'
boostraps = 1000
def f_bootstrap(values):
    return bs.bootstrap(values, stat_func=bs_stats.mean, num_iterations=boostraps, return_distribution=True)

df_metric_grouped = df_metric.groupby(cols)

df_metric_boots = df_metric_grouped.apply(lambda x: pd.Series({
    col_metric_value:
        f_bootstrap(x[col_metric_value].values)}
))
df_metric_boots = df_metric_boots.reset_index()

df_metric_boot_values = df_metric_boots[col_metric_value].apply(lambda x: pd.Series(x))

del df_metric_boots[col_metric_value]

df_metric_boot_values = pd.concat([df_metric_boots, df_metric_boot_values], axis=1)

df_li_boot_melted = pd.melt(df_metric_boot_values, value_vars=range(0, boostraps), id_vars=cols)


# mod = smf.gee("value ~  C(MSA_SCA_METHOD) + C(MSA_SCA_TYPE_DISCHARGE)",
#                   data=df_li_boot_melted,
#                   groups=df_li_boot_melted['MSA_SCA_YEAR'])
#
mod = smf.glm("value ~  C(MSA_SCA_METHOD) + C(MSA_SCA_YEAR)" ,
                  data=df_li_boot_melted)


res = mod.fit()
print(res.summary())

#
# cols = [
#     'MSA_SCA_METHOD',
#     'MSA_SCA_TYPE_DISCHARGE',
#     # 'MSA_SCA_TYPE_COMMUNITY_DETECTION',
#     'MSA_SCA_YEAR'
# ]
#
# # y = df_li['MSA_METRIC_VALUE'].values
# # X = df_li[cols].values
#
# # X = sm.add_constant(X)
# fam = sm.families.Gaussian()
# # ind = sm.cov_struct.Exchangeable()
#


# mod = smf.mixedlm("MSA_METRIC_VALUE ~ C(MSA_SCA_METHOD)  + C(MSA_SCA_YEAR)", data=df_li,  family=fam)


