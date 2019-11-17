__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.documents_mhs import GeocodeDocument
from mhs.src.library.file.file_reader import FileReaderFactory
from mhs.scripts.acquisition_script.script_acquisition import AcquisitionScript


class CensusGeocodesScript(AcquisitionScript):

    def __init__(self):
        super().__init__()

    def get_reader_type(self):
        return FileReaderFactory.TYPE_EXCEL

    def get_fields(self, **kwargs):
        return [
            'Summary Level',
            'State Code (FIPS)',
            'County Code (FIPS)',
            'County Subdivision Code (FIPS)',
            'Place Code (FIPS)',
            'Consolidtated City Code (FIPS)',
            'Area Name (including legal/statistical area description)'
        ]



    def get_prepared_item(self, script, item, **kwargs):

        # if 'Summary Level' not in item:
        #     return None
        obj = GeocodeDocument()

        obj.geocode_type = item['Summary Level']

        if obj.geocode_type not in GeocodeDocument.TYPES:
            return None

        obj.geocode_state_fips = item['State Code (FIPS)']

        if obj.geocode_type == GeocodeDocument.TYPE_STATE:
            obj.geocode_fips = item['State Code (FIPS)']
        elif obj.geocode_type == GeocodeDocument.TYPE_COUNTY:
            obj.geocode_fips = item['County Code (FIPS)']
        elif obj.geocode_type == GeocodeDocument.TYPE_COUNTY_SUBDIVISION:
            obj.geocode_fips = item['County Subdivision Code (FIPS)']
        elif obj.geocode_type == GeocodeDocument.TYPE_PLACE:
            obj.geocode_fips = item['Place Code (FIPS)']
        elif obj.geocode_type == GeocodeDocument.TYPE_CONSOLIDATED_CITY:
            obj.geocode_fips = item['Consolidtated City Code (FIPS)']
        obj.geocode_name = item['Area Name (including legal/statistical area description)']

        # obj.geocode_county = 'County Code (FIPS)',
        # obj.geocode_county_subdivision.db_field: 'County Subdivision Code (FIPS)',
        # obj.geocode_place.db_field: 'Place Code (FIPS)',
        # obj.geocode_consolidated_city.db_field: 'Consolidtated City Code (FIPS)',
        # obj.geocode_name.db_field: 'Area Name (including legal/statistical area description)',

        return obj

    def mongo_command(self, dict_document):
        self.documents.insert_census_geocode(dict_document)
