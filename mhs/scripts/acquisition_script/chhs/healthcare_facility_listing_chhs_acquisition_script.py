__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.documents_mhs import FacilityDocument
from mhs.src.library.file.file_reader import FileReaderFactory
from mhs.scripts.acquisition_script.script_acquisition import AcquisitionScript


class CHHSFacilityListingAcquisitionScript(AcquisitionScript):

    def __init__(self):
        super().__init__()

    def get_fields(self, **kwargs):
        return [
            'OSHPD_ID',
            'FACILITY_NAME',
            'LICENSE_TYPE_DESC',
            'LICENSE_CATEGORY_DESC',
            'DBA_ADDRESS1',
            'DBA_CITY',
            'DBA_ZIP_CODE',
            'COUNTY_NAME',
            'LATITUDE',
            'LONGITUDE',
        ]

    def get_reader_type(self):
        return FileReaderFactory.TYPE_CSV

    def mongo_command(self, dict_document):
        self.documents.insert_facility(dict_document)

    def get_prepared_item(self, script, item, **kwargs):
        obj = FacilityDocument()
        obj.facility_id = item['OSHPD_ID'][3:]
        obj.facility_id_type = FacilityDocument.TYPE_CHHS
        obj.facility_name = item['FACILITY_NAME']
        obj.facility_type = item['LICENSE_TYPE_DESC']
        obj.facility_subtype = item['LICENSE_CATEGORY_DESC']
        obj.address = item['DBA_ADDRESS1']
        obj.city = item['DBA_CITY']
        # obj.state = item['State']
        obj.zipcode = item['DBA_ZIP_CODE']
        obj.county_name = item['COUNTY_NAME']
        obj.coordinates = [float(item['LONGITUDE']), float(item['LATITUDE'])]

        # field_id = FacilityDocument.facility_id.db_field
        # item[field_id] = item[field_id]
        # item[FacilityDocument.facility_id_type.db_field] = FacilityDocument.TYPE_CHHS
        # db_geometry = FacilityDocument.coordinates.db_field
        # item[db_geometry] =
        return obj
