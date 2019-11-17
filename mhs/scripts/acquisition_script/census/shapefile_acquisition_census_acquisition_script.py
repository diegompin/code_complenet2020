__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.documents_mhs import ShapefileDocument
from mhs.src.library.file.file_reader import FileReaderFactory
from mhs.scripts.acquisition_script.script_acquisition import AcquisitionScript
import zipfile
import os
from shapely.geometry import mapping


class CensusShapefileScript(AcquisitionScript):

    def __init__(self):
        super().__init__()

    def get_reader_type(self):
        return FileReaderFactory.TYPE_SHAPEFILE

    def get_fields(self, **kwargs):
        return [
            'GEOID10',
            'geometry'
        ]
        # mapping = {
        #     ShapefileDocument.geocode_fips.db_field: 'GEOID10',
        #     ShapefileDocument.geocode_geometry.db_field: 'geometry'
        # }
        # return mapping

    def mongo_command(self, dict_document):
        self.documents.insert_census_shapefile(dict_document)

    def on_pre_mongo(self, script, **kwargs):
        ShapefileDocument.objects().delete()

    def on_post_acquire(self, script, **kwargs):
        out = script.get_folder_out(**kwargs)
        filename = script.get_file_name(**kwargs)
        extension = self.get_reader_type()
        zip_ref = zipfile.ZipFile(f'{out}/{filename}.{extension}', 'r')
        zip_ref.extractall(out)
        zip_ref.close()

        os.rename(f'{out}/{filename}.{extension}', f'{out}/{filename}.zip')
        for f in os.listdir(out):
            name = f.split('.')[0]
            extension = '.'.join(f.split('.')[1:])
            os.rename(f'{out}/{name}.{extension}', f'{out}/{filename}.{extension}')

    def get_prepared_item(self, script, item, **kwargs):
        obj = ShapefileDocument()
        obj.geocode_type = kwargs['config_key']
        obj.geocode_fips = item['GEOID10']
        # obj.geocode_geometry = mapping(item['geometry'])
        obj.geocode_geometry = item['geometry']
        return obj

    # def get_prepared_item(self, script, item, **kwargs):
    #     obj = ShapefileDocument()
    #     obj.geocode_fips
    #     # TODO UGLY! :-(
    #     geocode_type = kwargs['config_key']
    #     db_geotype = ShapefileDocument.geocode_type.db_field
    #     item[db_geotype] = geocode_type
    #     db_geometry = ShapefileDocument.geocode_geometry.db_field
    #     item[db_geometry] = mapping(item[db_geometry])
    #     return item
