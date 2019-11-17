__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'


from mhs.src.dao.mhs.documents_mhs import HospitalDischargeDocument

# from mhs.src.dao.mhs.dao_mongo import DAOMongo
from mhs.src.dao.base_dao import BaseDAO
import pandas as pd

# mod = DAOMongo.instance()


import locale

locale.setlocale(locale.LC_ALL, '')


def formatter_date(x):
    return x.strftime('%b-%d')


def formatter_number(x):
    return "$%s$" % locale.format_string("%d", x, grouping=True)


def formatter_decimal(x):
    return '%s\%%' % str.format('$%.2f$' % x)


#
# fields = ['HPD_FACILITY_NAME', 'HPD_PATIENT_ZIPCODE']
#
# dfs = [DAOMongo.get_counts(field) for field in fields]
#
# df_sum = DAOMongo.get_sum('HPD_FACILITY_NAME')
#
# dfs.append(df_sum)
#
# df = pd.concat(dfs, axis=1)
# df = df.reset_index()
# df = df.rename(
#     columns={
#         'HPD_DISCHARGE_YEAR': 'Year',
#         'HPD_DISCHARGE_TYPE': 'Type of Discharge',
#         'HPD_FACILITY_NAME': 'Number of Hospitals',
#         'HPD_PATIENT_ZIPCODE': 'Number of Patient Zip Codes',
#         'HPD_DISCHARGE_QUANTITY': 'Number of Discharges'
#     })
# df = df.set_index(['Year', 'Type of Discharge'])
# formatters = [formatter_number, formatter_number, formatter_number]
#
# latex = str(df.to_latex(header=True, index=True, multirow=True, formatters=formatters, escape=False))
#
# print(latex)

dao = BaseDAO(HospitalDischargeDocument)

discharges_match_dict = {
    # HospitalDischargeDocument.discharge_year: year,
    # HospitalDischargeDocument.discharge_type: type_discharge
}
discharges_project = [
    HospitalDischargeDocument.discharge_year,
    HospitalDischargeDocument.discharge_type,
    HospitalDischargeDocument.facility_id,
    HospitalDischargeDocument.patient_zcta,
    HospitalDischargeDocument.discharge_quantity
]

list_hospital_discharges = dao.obtain_pipeline(discharges_match_dict, discharges_project, math_none=False)

df = pd.DataFrame.from_dict(list_hospital_discharges)

df_group = df.groupby([HospitalDischargeDocument.discharge_year.db_field,
                       HospitalDischargeDocument.discharge_type.db_field,
                       HospitalDischargeDocument.facility_id.db_field,
                       HospitalDischargeDocument.patient_zcta.db_field])

df_group = df_group.sum()
df_group = df_group.reset_index()
df_group = df_group.groupby([HospitalDischargeDocument.discharge_type.db_field,
                             HospitalDischargeDocument.discharge_year.db_field,
                             ])

df_group = df_group.agg({
    HospitalDischargeDocument.facility_id.db_field: 'nunique',
    HospitalDischargeDocument.patient_zcta.db_field: 'nunique',
    HospitalDischargeDocument.discharge_quantity.db_field: 'sum'

})

df_group = df_group.reset_index()

dict_type_discharge = {HospitalDischargeDocument.TYPE_DISCHARGE_INPATIENT_FROM_ED: 0,
                       HospitalDischargeDocument.TYPE_DISCHARGE_INPANTIENT: 1,
                       HospitalDischargeDocument.TYPE_DISCHARGE_AS_ONLY: 2,
                       HospitalDischargeDocument.TYPE_DISCHARGE_ED_ONLY: 3}

df_group[HospitalDischargeDocument.discharge_type.db_field] = pd.Categorical(
    df_group[HospitalDischargeDocument.discharge_type.db_field],
    categories=sorted(dict_type_discharge, key=dict_type_discharge.get),
    ordered=True)
df_group = df_group.sort_values(by=[HospitalDischargeDocument.discharge_type.db_field])
df_latex = df_group.rename(
    columns={
        HospitalDischargeDocument.discharge_type.db_field: 'Type of Discharge',
        HospitalDischargeDocument.discharge_year.db_field: 'Year',
        HospitalDischargeDocument.facility_id.db_field: 'Number of Facilities',
        HospitalDischargeDocument.patient_zcta.db_field: 'Number of Patient ZCTAs',
        HospitalDischargeDocument.discharge_quantity.db_field: 'Number of Discharges'
    })

df_latex = df_latex.set_index(['Type of Discharge', 'Year'])
formatters = [formatter_number, formatter_number, formatter_number]

latex = str(df_latex.to_latex(header=True, index=True, multirow=True, formatters=formatters, escape=False))

print(latex)
