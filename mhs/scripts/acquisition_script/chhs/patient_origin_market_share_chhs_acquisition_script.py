__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.documents_mhs import HospitalDischargeDocument
from mhs.src.library.file.file_reader import FileReaderFactory
from mhs.scripts.acquisition_script.script_acquisition import AcquisitionScript
from mongoengine.queryset.visitor import Q, QCombination
from mhs.src.dao.mhs.discharges_dao import DischargesDAO


class CHHSPatientOriginMarketShareAcquisitionScript(AcquisitionScript):

    @staticmethod
    def instance(**kwargs):
        config_key = kwargs['config_key']
        if config_key == '2012_2013':
            return CHHS2012and2013PatientOriginMarketShareAcquisitionScript()
        elif config_key == '2014_2015':
            return CHHS2014and2015PatientOriginMarketShareAcquisitionScript()
        elif config_key == '2016_2017':
            return CHHS2016and2017PatientOriginMarketShareAcquisitionScript()
        elif config_key == '2018':
            return CHHS2018PatientOriginMarketShareAcquisitionScript()

    def __init__(self):
        super().__init__()
        self.discharges = DischargesDAO()

    def get_fields(self, **kwargs):
        return [
            'oshpd_id',
            'facility_name',
            'county_name',
            'pattype',
            'pzip',
            'pcounty',
            'discharges',
            'year'
        ]

    def get_query(self, years):
        queries = [Q(**{HospitalDischargeDocument.discharge_year.name: y}) for y in years]
        result = QCombination(QCombination.OR, queries)
        return result

    def get_reader_type(self):
        return FileReaderFactory.TYPE_EXCEL

    # def on_post_mongo(self, script, **kwargs):
    #     self.discharges.flush()

    def mongo_command(self, dict_document):
        self.discharges.insert_inpatient_discharge(dict_document)

    def get_prepared_item(self, script, item, **kwargs):
        obj = HospitalDischargeDocument()
        obj.facility_id = item['oshpd_id']
        obj.facility_name = item['facility_name']
        obj.facility_county = item['county_name']
        obj.discharge_type = item['pattype']
        obj.patient_zipcode = item['pzip']
        obj.patient_county = item['pcounty']
        obj.discharge_quantity = item['discharges']
        obj.discharge_year = item['year']
        return obj


class CHHS2012and2013PatientOriginMarketShareAcquisitionScript(CHHSPatientOriginMarketShareAcquisitionScript):
    def on_pre_mongo(self, script, **kwargs):
        query = self.get_query([2012, 2013])
        HospitalDischargeDocument.objects(query).delete()


class CHHS2014and2015PatientOriginMarketShareAcquisitionScript(CHHSPatientOriginMarketShareAcquisitionScript):
    def on_pre_mongo(self, script, **kwargs):
        query = self.get_query([2014, 2015])
        HospitalDischargeDocument.objects(query).delete()


class CHHS2016and2017PatientOriginMarketShareAcquisitionScript(CHHSPatientOriginMarketShareAcquisitionScript):
    def on_pre_mongo(self, script, **kwargs):
        query = self.get_query([2016, 2017])
        HospitalDischargeDocument.objects(query).delete()


class CHHS2018PatientOriginMarketShareAcquisitionScript(CHHSPatientOriginMarketShareAcquisitionScript):

    def get_fields(self, **kwargs):
        return [
            'oshpd_id',
            'pattype',
            'PZIP',
            'PZIP',
            'pcounty',
            'FACILITY_NAME',
            'COUNTY_NAME',
            'discharges',
            'year'
        ]

    def get_prepared_item(self, script, item, **kwargs):
        obj = HospitalDischargeDocument()
        obj.facility_id = item['oshpd_id'][-6:].rjust(6, '0')
        obj.facility_name = item['FACILITY_NAME']
        obj.facility_county = item['COUNTY_NAME']
        obj.discharge_type = item['pattype']
        obj.patient_zipcode = item['PZIP']
        obj.patient_county = item['COUNTY_NAME']
        obj.discharge_quantity = item['discharges']
        obj.discharge_year = item['year']

        return obj

    def on_pre_mongo(self, script, **kwargs):
        query = self.get_query([2018])
        HospitalDischargeDocument.objects(query).delete()
