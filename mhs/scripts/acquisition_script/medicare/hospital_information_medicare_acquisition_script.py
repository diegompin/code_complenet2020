__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.documents_mhs import FacilityDocument
from mhs.src.library.file.file_reader import FileReaderFactory
from mhs.scripts.acquisition_script.script_acquisition import AcquisitionScript


class MedicareHospitalGeneralInformationAcquisitionScript(AcquisitionScript):

    def __init__(self):
        super().__init__()

    def get_fields(self, **kwargs):
        return [
            'Provider ID',
            'Hospital Name',
            'Hospital Type',
            'Address',
            'City',
            'State',
            'ZIP Code',
            'County Name'
        ]

    def get_reader_type(self):
        return FileReaderFactory.TYPE_CSV

    def mongo_command(self, dict_document):
        self.documents.insert_facility(dict_document)

    def get_prepared_item(self, script, item, **kwargs):
        obj = FacilityDocument()
        obj.facility_id_type = FacilityDocument.TYPE_CMS
        obj.facility_id = item['Provider ID']
        obj.facility_name = item['Hospital Name']
        obj.facility_type = item['Hospital Type']
        obj.address = item['Address']
        obj.city = item['City']
        obj.state = item['State']
        obj.zipcode = item['ZIP Code']
        obj.county_name = item['County Name']
        return obj
