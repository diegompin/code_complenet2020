import csv
import json
import pandas as pd
import geopandas as geo
from openpyxl import load_workbook


class FileReaderFactory(object):
    TYPE_CSV = 'csv'
    TYPE_JSON = 'json'
    TYPE_EXCEL = 'xlsx'
    TYPE_SHAPEFILE = 'shp'
    TYPE_TXT = 'txy'

    def get_reader(self, file_type):
        if file_type == self.TYPE_CSV or file_type == self.TYPE_TXT:
            return CsvFileReader()
        elif file_type == self.TYPE_JSON:
            return JsonFileReader()
        elif file_type == self.TYPE_EXCEL:
            return ExcelFileReader()
        elif file_type == self.TYPE_SHAPEFILE:
            return ShapeFileReader()
        else:
            raise Exception(f'File type {file_type} not implemented')


class FileBaseReader(object):

    def __init__(self):
        pass

    def extension(self):
        raise NotImplemented('method read not implemented')

    def read(self, *args, **kwargs):
        raise NotImplemented('method read not implemented')

    # def get_mapping(self):
    #     return None

    @staticmethod
    def get_dict(npi_columns, npi_element):
        npi_dict = dict()
        for k in npi_columns:
            if type(k[1]) == str:
                npi_dict[k[0]] = npi_element[k[1]]
            else:
                npi_dict[k[0]] = k[1](npi_element)
        return npi_dict

    def get_fields(self, **kwargs):
        mapping = None
        if 'fields' in kwargs:
            mapping = kwargs['fields']
        return mapping


class ShapeFileReader(FileBaseReader):

    def __init__(self):
        super().__init__()

    def extension(self):
        return '.shp'

    def read(self, *args, **kwargs):
        file_name = kwargs['file_name']
        fields = self.get_fields(**kwargs)

        df = geo.read_file(file_name)

        df.columns = [e.strip() for e in list(df.columns)]
        if fields is not None:
            df = df[list(fields)]
        #     df = df.rename(columns=dict(zip(fields.values(), fields.keys())))

        documents = df.to_dict(orient='records')

        for doc in documents:
            yield doc


class CsvFileReader(FileBaseReader):

    def __init__(self):
        super().__init__()

    def extension(self):
        return '.csv'

    def read(self, *args, **kwargs):
        file_name = kwargs['file_name']

        fields = self.get_fields(**kwargs)
        df = pd.read_csv(file_name, delimiter=',', header=0, dtype=str, keep_default_na=False)
        # df = pd.read_csv(file_name, delimiter=',', header=0, encoding=self.encoding)
        df.columns = [e.strip() for e in list(df.columns)]
        # if fields is not None:
        #     df = df[list(fields.values())]
        #     df = df.rename(columns=dict(zip(fields.values(), fields.keys())))

        # df = df.fillna('')
        documents = df.to_dict(orient='records')

        for doc in documents:
            yield doc


class JsonFileReader(FileBaseReader):

    def __init__(self):
        super().__init__()


class ExcelFileReader(FileBaseReader):

    def __init__(self):
        super().__init__()

    def extension(self):
        return '.excel'

    def read(self, *args, **kwargs):

        file_name = kwargs['file_name']
        sheet_name = kwargs['sheet_name']

        # df = pd.read_excel(file_name, sheet_name=sheet_name, nrows=100)
        wb = load_workbook(filename=file_name, read_only=True)
        if sheet_name in wb:
            ws = wb[sheet_name]
        elif sheet_name.title() in wb:
            ws = wb[sheet_name.title()]
        else:
            raise Exception('sheet_name not found')

        fields = self.get_fields(**kwargs)
        header = None
        reached_line = False
        for row in ws.rows:
            # dict_element = None
            cell_content = [cell.value for cell in row]
            if any(cell in fields for cell in cell_content):
                reached_line = True
            if not reached_line:
                continue
            if not header:
                header = [cell.value for cell in row]
                continue

            values = [cell.value for cell in row]
            dict_element = dict(zip(header, values))
            # if fields:
            #     dict_element = self.get_dict(fields, dict_element)
            yield dict_element
            # yield values
