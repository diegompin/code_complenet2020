import pandas as pd
import numpy as np

from mhs.src.shared_care_areas.metrics.base import BaseMetricsSharedCareArea
from mhs.src.dao.base_dao import BaseDAO
# from mhs.src.dao.discharges_dao import DischargesDAO
from mhs.src.dao.mhs.documents_mhs import (SharedCareArea,
                                           HospitalDischargeDocument,
                                           ZctaCountyDocument)
from mhs.src.dao.mhs.documents_mhs import MetricsSharedCareAreaDocument


class DischargeBasedMetricsSharedCareArea(BaseMetricsSharedCareArea):
    """
    DischargeBasedMetricsSharedCareArea
    """

    def __init__(self, name):
        super().__init__(name)
        self.__dao_discharges__ = BaseDAO(HospitalDischargeDocument)

    def get_discharges(self, type_discharge, year):
        discharges_match_dict = {
            HospitalDischargeDocument.discharge_year: year,
            HospitalDischargeDocument.discharge_type: type_discharge
        }
        discharges_project = [
            HospitalDischargeDocument.patient_zcta,
            HospitalDischargeDocument.facility_zcta,
            HospitalDischargeDocument.discharge_quantity
        ]
        list_hospital_discharges = self.__dao_discharges__.obtain_pipeline(discharges_match_dict, discharges_project,
                                                                           math_none=False)
        df_hospital_discharges = pd.DataFrame.from_dict(list_hospital_discharges)
        return df_hospital_discharges

    def calculate_metric(self, df_hospital_discharges, df_shared_care_areas):
        raise NotImplemented()

    def get_metric(self, method, type_discharge, year, type_community_detection, normalized=None):
        df_hospital_discharges = self.get_discharges(type_discharge, year)
        df_shared_care_areas = self.get_shared_care_areas(method, type_community_detection,
                                                          type_discharge, year)
        df_sca = self.calculate_metric(df_hospital_discharges, df_shared_care_areas)

        if normalized:
            df_sca = self.get_normalized(df_sca, df_hospital_discharges, df_shared_care_areas)

        return df_sca

    # def get_metric_normalized(self, method, type_discharge, year, type_community_detection):
    #     df_hospital_discharges = self.get_discharges(type_discharge, year)
    #     df_shared_care_areas = self.get_shared_care_areas(method, type_community_detection, type_discharge, year)
    #     df_sca = self.calculate_metric(df_hospital_discharges, df_shared_care_areas)
    #
    #
    #     return df_sca

    def get_normalized(self, df_sca, df_hospital_discharges, df_shared_care_areas):
        repetitions = 100
        # sca_r = list()
        sca_r = np.zeros(repetitions)
        for i in range(repetitions):
            df_shared_care_areas_r = df_shared_care_areas.copy()
            df_shared_care_areas_r['SCA_ID'] = np.random.permutation(df_shared_care_areas['SCA_ID'])
            df_sca_r = self.calculate_metric(df_hospital_discharges, df_shared_care_areas_r)
            sca_r[i] = np.mean(df_sca_r['metric_value'].values)
            del df_shared_care_areas_r
        sca_r = np.ravel(sca_r)
        # bootstraps_sca = f_bootstrap(df_sca['metric_value'].values)
        # bootstraps_sca_r = f_bootstrap(sca_r)
        mean_r = np.mean(sca_r)
        std_r = np.std(sca_r)
        col_metric_value = MetricsSharedCareAreaDocument.metric_value.name
        estimates = df_sca[col_metric_value]
        z_score = (estimates - mean_r) / std_r
        df_sca[col_metric_value] = z_score
        return df_sca


class NumberDischargesMetricsSharedCareArea(DischargeBasedMetricsSharedCareArea):
    NAME = 'NUMBER_DISCHARGES'

    def __init__(self):
        super().__init__(self.NAME)

    def calculate_metric(self, df_hospital_discharges, df_shared_care_areas):
        df_sca_discharge = df_hospital_discharges.merge(df_shared_care_areas, how='inner', left_on='HPD_PATIENT_ZCTA',
                                                        right_on='SCA_ZCTA')

        # df_sca_discharge = df_sca_discharge.merge(df_shared_care_areas, how='inner', left_on='HPD_FACILITY_ZCTA',
        #                                           right_on='SCA_ZCTA', suffixes=('_PATIENT_ZCTA', '_FACILITY_ZCTA'))

        # df_sca_sca = df_sca_discharge \
        #     .groupby(['SCA_ID_PATIENT_ZCTA', 'SCA_ID_FACILITY_ZCTA']) \
        #     .sum() \
        #     .reset_index()

        df_sca = df_sca_discharge.groupby(['SCA_ID']).sum()

        df_sca[MetricsSharedCareAreaDocument.metric_value.name] = df_sca['HPD_DISCHARGE_QUANTITY']

        df_sca = df_sca.reset_index()
        df_sca = df_sca.rename(columns={
            'SCA_ID': MetricsSharedCareAreaDocument.sca_id.name,
        })

        df_sca = self.get_all_scas(df_shared_care_areas, df_sca)

        df_sca = df_sca.fillna(0)

        return df_sca


class LocalizationIndexMetricsSharedCareArea(DischargeBasedMetricsSharedCareArea):
    """
    LI describes the proportion of patients that are treated in the same HSA/HRR as where they live,
    and it is designed to capture the propensity of patients visiting local hospitals
    (Guagliardo et al. 2004). A higher LI demonstrates more accurate or representative delineation
    of HSA or HRR boundaries.
    """

    NAME = 'DISCHARGE_LOCALIZATION_INDEX'

    def __init__(self):
        super().__init__(self.NAME)

    def calculate_metric(self, df_hospital_discharges, df_shared_care_areas):
        df_sca_discharge = df_hospital_discharges.merge(df_shared_care_areas, how='inner', left_on='HPD_PATIENT_ZCTA',
                                                        right_on='SCA_ZCTA')

        df_sca_discharge = df_sca_discharge.merge(df_shared_care_areas, how='inner', left_on='HPD_FACILITY_ZCTA',
                                                  right_on='SCA_ZCTA', suffixes=('_PATIENT_ZCTA', '_FACILITY_ZCTA'))
        df_sca_sca = df_sca_discharge \
            .groupby(['SCA_ID_PATIENT_ZCTA', 'SCA_ID_FACILITY_ZCTA']) \
            .sum() \
            .reset_index()
        df_sca = df_sca_sca.groupby(['SCA_ID_PATIENT_ZCTA']).sum()
        df_sca_self = df_sca_sca \
            .loc[df_sca_sca['SCA_ID_PATIENT_ZCTA'] == df_sca_sca['SCA_ID_FACILITY_ZCTA']] \
            .drop('SCA_ID_FACILITY_ZCTA', axis=1) \
            .set_index('SCA_ID_PATIENT_ZCTA')
        df_sca = df_sca.join(df_sca_self, lsuffix='_TOTAL', rsuffix='_SELF')

        df_sca[MetricsSharedCareAreaDocument.metric_value.name] = df_sca['HPD_DISCHARGE_QUANTITY_SELF'] / df_sca[
            'HPD_DISCHARGE_QUANTITY_TOTAL']
        df_sca = df_sca.reset_index()
        df_sca = df_sca.rename(columns={
            'SCA_ID_PATIENT_ZCTA': MetricsSharedCareAreaDocument.sca_id.name,
        })

        df_sca = self.get_all_scas(df_shared_care_areas, df_sca)

        df_sca = df_sca.fillna(0)

        return df_sca


class MarketShareIndex(DischargeBasedMetricsSharedCareArea):
    """
    MSI is the proportion of HSA or HRR patients who do not live in the regions (Kilaru et al. 2015).
    It represents the tendency of hospitals in an HSA (or HRR) to absorb out-of-area residents;
    hence, a lower value generally means better delineation
    """

    NAME = 'DISCHARGE_MARKET_SHARED_INDEX'

    def __init__(self):
        super().__init__(self.NAME)

    def calculate_metric(self, df_hospital_discharges, df_shared_care_areas):
        df_sca_discharge = df_hospital_discharges.merge(df_shared_care_areas, how='inner', left_on='HPD_PATIENT_ZCTA',
                                                        right_on='SCA_ZCTA')
        df_sca_discharge = df_sca_discharge.merge(df_shared_care_areas, how='inner', left_on='HPD_FACILITY_ZCTA',
                                                  right_on='SCA_ZCTA', suffixes=('_PATIENT_ZCTA', '_FACILITY_ZCTA'))
        df_sca_sca = df_sca_discharge \
            .groupby(['SCA_ID_PATIENT_ZCTA', 'SCA_ID_FACILITY_ZCTA']) \
            .sum() \
            .reset_index()

        df_sca_total = df_sca_sca.groupby(['SCA_ID_FACILITY_ZCTA']).sum()

        df_sca_other = df_sca_sca \
            .loc[df_sca_sca['SCA_ID_PATIENT_ZCTA'] != df_sca_sca['SCA_ID_FACILITY_ZCTA']] \
            .drop('SCA_ID_PATIENT_ZCTA', axis=1) \
            .groupby('SCA_ID_FACILITY_ZCTA') \
            .sum()

        df_sca_total = df_sca_total.join(df_sca_other, lsuffix='_TOTAL', rsuffix='_OTHER')
        df_sca_total[MetricsSharedCareAreaDocument.metric_value.name] = (
                df_sca_total['HPD_DISCHARGE_QUANTITY_OTHER'] / df_sca_total['HPD_DISCHARGE_QUANTITY_TOTAL']
        )
        df_sca_total = df_sca_total.reset_index()
        df_sca_total = df_sca_total.rename(columns={
            'SCA_ID_FACILITY_ZCTA': MetricsSharedCareAreaDocument.sca_id.name,
        })

        df_sca_total = self.get_all_scas(df_shared_care_areas, df_sca_total)

        df_sca_total = df_sca_total.fillna(1)

        return df_sca_total


class NetPatientFlow(DischargeBasedMetricsSharedCareArea):
    """
    NPF is defined as the ratio of incoming patients to outgoing patients,
    that is, the non-HSA (or HRR) residents treated inside the region
    versus HSA (or HRR) residents treated outside the region (Klauss et al. 2005).
    A value >1 indicates the tendency of more patients traveling inside the area to seek hospital care,
    while a value <1 implies the tendency of more patients leaving the area for hospital care.
    """

    NAME = 'DISCHARGE_NET_PATIENT_FLOW'

    def __init__(self):
        super().__init__(self.NAME)

    def calculate_metric(self, df_hospital_discharges, df_shared_care_areas):
        df_sca_discharge = df_hospital_discharges.merge(df_shared_care_areas, how='inner', left_on='HPD_PATIENT_ZCTA',
                                                        right_on='SCA_ZCTA')
        df_sca_discharge = df_sca_discharge.merge(df_shared_care_areas, how='inner', left_on='HPD_FACILITY_ZCTA',
                                                  right_on='SCA_ZCTA', suffixes=('_PATIENT_ZCTA', '_FACILITY_ZCTA'))
        df_sca_sca = df_sca_discharge \
            .groupby(['SCA_ID_PATIENT_ZCTA', 'SCA_ID_FACILITY_ZCTA']) \
            .sum() \
            .reset_index()

        df_sca_other = df_sca_sca \
            .loc[df_sca_sca['SCA_ID_PATIENT_ZCTA'] != df_sca_sca['SCA_ID_FACILITY_ZCTA']] \
            .drop('SCA_ID_PATIENT_ZCTA', axis=1) \
            .groupby('SCA_ID_FACILITY_ZCTA') \
            .sum() \
            .reset_index()

        df_sca_self = df_sca_sca \
            .loc[df_sca_sca['SCA_ID_PATIENT_ZCTA'] != df_sca_sca['SCA_ID_FACILITY_ZCTA']] \
            .drop('SCA_ID_FACILITY_ZCTA', axis=1) \
            .groupby('SCA_ID_PATIENT_ZCTA') \
            .sum() \
            .reset_index()

        df_sca = df_sca_other.merge(df_sca_self, how='left', left_on='SCA_ID_FACILITY_ZCTA',
                                    right_on='SCA_ID_PATIENT_ZCTA', suffixes=('_OTHER', '_SELF'))

        df_sca[MetricsSharedCareAreaDocument.metric_value.name] = df_sca['HPD_DISCHARGE_QUANTITY_OTHER'] / df_sca[
            'HPD_DISCHARGE_QUANTITY_SELF']
        # df_sca = df_sca_sca.groupby(['SCA_ID_PATIENT_ZCTA']).sum()
        # df_sca = df_sca.join(df_sca_self, lsuffix='_TOTAL', rsuffix='_SELF')
        # df_sca[MetricsSharedCareAreaDocument.metric_value.name] = df_sca['HPD_DISCHARGE_QUANTITY_SELF'] / df_sca[
        #     'HPD_DISCHARGE_QUANTITY_TOTAL']
        # df_sca = df_sca.reset_index()

        df_sca = df_sca.rename(columns={
            'SCA_ID_PATIENT_ZCTA': MetricsSharedCareAreaDocument.sca_id.name,
        })
        df_sca = self.get_all_scas(df_shared_care_areas, df_sca)

        df_sca = df_sca.fillna(1)

        return df_sca
