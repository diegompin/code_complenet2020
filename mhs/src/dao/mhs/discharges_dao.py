__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.dao_mongo import HospitalDischargeDocument
from mhs.src.dao.base_dao import *
from mhs.src.dao.mhs.cache_dao import CacheDAO
import logging


class DischargesDAO(BaseDAO):

    def __init__(self):
        super().__init__(HospitalDischargeDocument)
        self.facility_cache = CacheDAO(FacilityDocument)
        self.zipzcta_cache = CacheDAO(ZipcodeZctaDocument)

    def insert_inpatient_discharge(self, inpatient_discharge):
        doc_new = HospitalDischargeDocument.from_json(json.dumps(inpatient_discharge))

        df_facilities = self.facility_cache.obtain_cache()
        df_zip_zcta = self.zipzcta_cache.obtain_cache()

        facility_zipcode = df_facilities.loc[(
                (df_facilities['FAC_ID_TYPE'] == FacilityDocument.TYPE_CHHS) &
                (df_facilities['FAC_ID'] == doc_new.facility_id)
        )
        ]['FAC_ZIPCODE'].values

        if len(facility_zipcode):
            facility_zipcode = facility_zipcode[0]
            doc_new.facility_zipcode = facility_zipcode
            facility_zcta = df_zip_zcta.loc[
                (df_zip_zcta['ZZA_ZIPCODE'] == facility_zipcode)
            ]['ZZA_ZCTA'].values

            if len(facility_zcta):
                facility_zcta = facility_zcta[0]
                doc_new.facility_zcta = facility_zcta
        patient_zcta = df_zip_zcta.loc[df_zip_zcta['ZZA_ZIPCODE'] == doc_new.patient_zipcode]['ZZA_ZCTA'].values

        if len(patient_zcta):
            patient_zcta = patient_zcta[0]
            doc_new.patient_zcta = patient_zcta
        # self.insert_bulk(doc_new)
        doc_new.save()

    def obtain_pipeline(self, dict_match, dict_project=None, math_none=False):
        super().obtain_pipeline(dict_match=dict_match,
                                dict_project=dict_project,
                                math_none=True)


'''

self = DischargesDAO()
facility_zipcode = df_facilities.loc[(df_facilities['FAC_ID_TYPE'] == FacilityDocument.TYPE_CHHS) & (
                df_facilities['FAC_ID'] == '010376')]['FAC_ZIPCODE'].values
if len(facility):
    print(True)
         
 facility_zcta = df_zip_zcta.loc[df_zip_zcta['ZZA_ZIPCODE'] == facility_zipcode]['ZZA_ZCTA'].values
facility_zcta = facility_zcta[0]
patient_zcta = df_zip_zcta.loc[df_zip_zcta['ZZA_ZIPCODE'] == '95831']['ZZA_ZCTA'].values
  

'''

# def obtain_discharges(self, dict_match):
#     dict_match = self.build_pipeline(dict_match)
#
#     result = HospitalDischargeDocument.objects(**dict_match)
#
#     return result
#
# def obtain_discharges_pipeline(self, dict_match, dict_project=None):
#     list_pipeline = list()
#
#     dict_match = self.build_pipeline_match(dict_match)
#     list_pipeline.append(dict_match)
#     if dict_project:
#         dict_project = self.build_project(dict_project)
#     else:
#         dict_project = self.build_project([
#             HospitalDischargeDocument.facility_id,
#             HospitalDischargeDocument.facility_name,
#             HospitalDischargeDocument.facility_zipcode,
#             HospitalDischargeDocument.facility_zcta,
#             HospitalDischargeDocument.patient_zipcode,
#             HospitalDischargeDocument.patient_zcta,
#             HospitalDischargeDocument.discharge_quantity
#         ])
#
#     list_pipeline.append(dict_project)
#
#     result = HospitalDischargeDocument.objects.aggregate(*list_pipeline)
#
#     return result

# def get_discharge_years(self):
#     return HospitalDischargeDocument.objects().distinct(field=HospitalDischargeDocument.discharge_year.name)
#
# def get_dischareg_types(self):
#     return HospitalDischargeDocument.objects().distinct(field=HospitalDischargeDocument.discharge_type.name)
#
