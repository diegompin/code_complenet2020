from mhs.src.dao.mhs.documents_mhs import SharedCareArea

__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.base_dao import BaseDAO
from mhs.src.shared_care_areas.metrics.discharge_based import (
    LocalizationIndexMetricsSharedCareArea,
    MarketShareIndex,
    NetPatientFlow,
    NumberDischargesMetricsSharedCareArea
)

from mhs.src.shared_care_areas.metrics.network_based import (
    ConductanceMetricsSharedCareArea,
    NumberCommunitiesMetricsSharedCareArea
)


class MetricsSharedCareAreaFactory(object):
    TYPE_LOCALIZATION_INDEX = LocalizationIndexMetricsSharedCareArea.NAME
    TYPE_MARKET_SHARED_INDEX = MarketShareIndex.NAME
    TYPE_NET_PATIENT_FLOW = NetPatientFlow.NAME
    TYPE_NUMBER_DISCHARGES = NumberDischargesMetricsSharedCareArea.NAME
    TYPE_CONDUCTANCE = ConductanceMetricsSharedCareArea.NAME
    TYPE_NUMBER_COMMUNITIES = NumberCommunitiesMetricsSharedCareArea.NAME

    TYPES = [
        TYPE_LOCALIZATION_INDEX,
        TYPE_MARKET_SHARED_INDEX,
        TYPE_NET_PATIENT_FLOW,
        TYPE_NUMBER_DISCHARGES,
        TYPE_CONDUCTANCE,
        TYPE_NUMBER_COMMUNITIES
    ]

    @staticmethod
    def instance(metric):
        if metric == MetricsSharedCareAreaFactory.TYPE_LOCALIZATION_INDEX:
            return LocalizationIndexMetricsSharedCareArea()
        elif metric == MetricsSharedCareAreaFactory.TYPE_MARKET_SHARED_INDEX:
            return MarketShareIndex()
        elif metric == MetricsSharedCareAreaFactory.TYPE_NET_PATIENT_FLOW:
            return NetPatientFlow()
        elif metric == MetricsSharedCareAreaFactory.TYPE_NUMBER_DISCHARGES:
            return NumberDischargesMetricsSharedCareArea()
        elif metric == MetricsSharedCareAreaFactory.TYPE_CONDUCTANCE:
            return ConductanceMetricsSharedCareArea()
        elif metric == MetricsSharedCareAreaFactory.TYPE_NUMBER_COMMUNITIES:
            return NumberCommunitiesMetricsSharedCareArea()
        else:
            raise NotImplemented(metric)
