__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.documents_mhs import ZctaCountyDocument
from mhs.src.library.file.file_reader import FileReaderFactory
from mhs.scripts.acquisition_script.script_acquisition import AcquisitionScript


class CensusRelationshipZCTACountyScript(AcquisitionScript):

    def __init__(self):
        super().__init__()

    def get_reader_type(self):
        return FileReaderFactory.TYPE_CSV

    def get_fields(self, **kwargs):
        return [
            'ZCTA5',
            'COUNTY',
            'STATE',
            'ZPOPPCT',
            'COPOPPCT',
        ]
        # mapping = {
        #     ZctaCountyDocument.geocode_fips.db_field: 'ZCTA5',
        #     ZctaCountyDocument.geocode_fips_county.db_field: 'COUNTY',
        #     ZctaCountyDocument.geocode_fips_state.db_field: 'STATE',
        #     ZctaCountyDocument.percentage_zcta.db_field: 'ZPOPPCT',
        #     ZctaCountyDocument.percentage_relationship.db_field: 'COPOPPCT',
        # }
        # return mapping

    def get_prepared_item(self, script, item, **kwargs):
        obj = ZctaCountyDocument()
        obj.geocode_fips = str(item['ZCTA5'])
        obj.geocode_fips_county = item['COUNTY']
        obj.geocode_fips_state = item['STATE']
        obj.percentage_zcta = float(item['ZPOPPCT'])
        obj.percentage_relationship = float(item['COPOPPCT'])
        return obj

    def mongo_command(self, dict_document):
        self.documents.insert_census_relationship_zcta_county(dict_document)
