__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.src.dao.mhs.documents_mhs import ZipcodeZctaDocument
from mhs.src.library.file.file_reader import FileReaderFactory
from mhs.scripts.acquisition_script.script_acquisition import AcquisitionScript


class ScriptUSDSMapper(AcquisitionScript):

    def __init__(self):
        super().__init__()

    def get_reader_type(self):
        return FileReaderFactory.TYPE_EXCEL

    def get_fields(self, **kwargs):
        return [
            'ZIP_CODE',
            'ZCTA',
            'STATE',
            'PO_NAME'
        ]

    def on_pre_mongo(self, script, **kwargs):
        ZipcodeZctaDocument.drop_collection()

    def get_prepared_item(self, script, item,  **kwargs):
        obj = ZipcodeZctaDocument()
        obj.zipcode = str(item['ZIP_CODE'])
        obj.zcta = str(item['ZCTA'])
        obj.state = item['STATE']
        obj.city = item['PO_NAME']
        return obj

    def mongo_command(self, dict_document):
        self.documents.insert_zip_zcta(dict_document)
