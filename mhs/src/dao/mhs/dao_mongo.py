__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from mhs.config import MHSConfigManager
from mhs.src.dao.mhs.documents_mhs import *
from pymongo.errors import BulkWriteError
from mongoengine.queryset.visitor import Q, QCombination
import json
import pandas as pd
import os
import threading
import multiprocessing
import logging

class DAOMongo(object):

    __singleton_lock = multiprocessing.Lock()
    __singleton_instance = None

    def __init__(self):
        logging.info('INIT DAOMONGO')
        # self.config = configparser.ConfigParser()
        self.config = MHSConfigManager()
        # self.connection = connect('mhs')
        # self.connection = connect('mhs', host='mongo', authentication_source='mhs')
        # self.connection = connect('mhs', username='user', password='pwd', host='localhost', authentication_source='mhs')
        # self.connection = connect('mhs', username='user', password='pwd', host='mongo', authentication_source='mhs')

        port = int(os.environ['MONGO_PORT'])
        host = os.environ['MONGO_HOST']

        self.connection = connect('mhs', port=port, username='mhs', password='mhs', host=host,
                                  authentication_source='mhs')


        def attach_subclass(parent, func):
            for cls in parent.__subclasses__():
                if hasattr(cls, func):
                    method_to_call = getattr(cls, func)
                    signal = getattr(signals, func)
                    signal.connect(method_to_call, sender=cls)
                    # signals.pre_init.connect(method_to_call, sender=cls)
                attach_subclass(cls, func)

        for event in signals.__all__:
            attach_subclass(MHSBaseDocumentAudit, event)

        # self.config.read('mhs.cfg')
        # self.get_mongo()
        # TODO Make general

    @classmethod
    def instance(cls):
        if not cls.__singleton_instance:
            with cls.__singleton_lock:
                if not cls.__singleton_instance:
                    # cls.__singleton_instance = object.__init__(cls)
                    # cls.__singleton_instance = object.__new__(cls)

                    cls.__singleton_instance = cls()

        return cls.__singleton_instance

    # def get_mongo(self):
    #
    #     # host = config_mongo['mongo_host']
    #     # port = config_mongo['port']
    #     # database = config_mongo['database']
    #     database = self.config.mhs_mongo
    #     # print(f'database: {database}')
    #     connect(database)

    # def get_params(self):
    #     # self.documents
    #     # self.log.info('Entering params')
    #     params = self.get_config_inpatient_discharge()
    #     ret = ((p.config_key, p.config_value) for p in params)
    #     return ret

    @staticmethod
    def get_query(document, fields, is_and=True):
        filds_name = [f.name for f in fields]
        queries = map(lambda i: Q(**{i[0]: i[1]}),
                      [d for d in document._data.items() if d[0] in filds_name])
        query = QCombination(QCombination.AND, queries)
        if not is_and:
            query = QCombination(QCombination.OR, queries)
        return query

    def get_config_inpatient_discharge(self):
        # query = Q(config_class="CHHS", config_name="PATIENT_ORIGIN_MARKET_SHARE")
        # query_result = ConfigDocument.objects(query)
        # results = ((item.config_key, item.config_value) for item in query_result)
        # return iter(results)
        return self.get_config(config_class="CHHS", config_name="PATIENT_ORIGIN_MARKET_SHARE")

    def get_inpatient_discharge(self):
        query = Q(discharge_year=2014)
        query_result = HospitalDischargeDocument.objects(query)
        results = ((item.patient_zipcode, item.facility_id, item.discharge_quantity, item.discharge_type) for item
                   in query_result)
        return iter(results)

    def get_config_census_shapefile(self):
        # query = Q(config_class="CENSUS", config_name="SHAPEFILE")
        # query_result = ConfigDocument.objects(query)
        # results = ((item.config_key, item.config_value) for item in query_result)
        # return iter(results)
        return self.get_config(config_class="CENSUS", config_name="SHAPEFILE")

    def get_config_census_geocodes(self, **kwargs):
        # query = Q(config_class="CENSUS", config_name="GEOCODES")
        # query_result = ConfigDocument.objects(query)
        # results = ((item.config_key, item.config_value) for item in query_result)
        # return iter(results)
        return self.get_config(config_class="CENSUS", config_name="GEOCODES")

    def get_config(self, **kwargs):
        # if 'config_class' in kwargs:
        query = Q(**kwargs)
        query_result = ConfigDocument.objects(query)
        results = ((item.config_key, item.config_value) for item in query_result)
        # kwargs
        return iter(results)

    def get_config_udsmapper_zip_zcta(self, **kwargs):
        # query = Q(config_class="UDSMapper", config_name="ZIP_ZCTA")
        # query_result = ConfigDocument.objects(query)
        # results = ((item.config_key, item.config_value) for item in query_result)
        # return iter(results)
        return self.get_config(config_class="UDSMapper", config_name="ZIP_ZCTA")

    def insert_zip_zcta(self, document):
        errors = []
        try:
            doc_new = ZipcodeZctaDocument.from_json(json.dumps(document))
            doc_found = self.find_zip_zcta(doc_new)
            # doc_found: HospitalDischargeDocument
            if doc_found:
                doc_found.zcta = doc_new.zcta
                doc_found.state = doc_new.state
                doc_found.city = doc_new.city
                doc_found.save()
            else:
                doc_new.save()

            #
            #
            # new_inpatient_discharge = HospitalDischargeDocument()
            # new_inpatient_discharge.facility_id_oshpd = inpatient_discharge['FAC_ID']
            # new_inpatient_discharge.facility_name = inpatient_discharge['FAC_NAME']
            # new_inpatient_discharge.patient_zipcode = inpatient_discharge['PAT_ZIP']
            # new_inpatient_discharge.discharge_quantity = inpatient_discharge['DIS_TOTAL']
            # new_inpatient_discharge.discharge_type = inpatient_discharge['PAT_TYPE']
            # new_inpatient_discharge.discharge_year = int(inpatient_discharge['DIS_YEAR'])
            # new_inpatient_discharge.save()
        except Exception as e:
            errors.append(e)

        if errors:
            raise Exception(errors)

    def insert_census_relationship_zcta_county(self, document):
        errors = []
        try:
            doc_new = ZctaCountyDocument.from_json(json.dumps(document))
            # assert isinstance(doc_new, ZctaCountyDocument)
            doc_found = self.find_census_relationship_zcta_county(doc_new)
            # assert isinstance(doc_found, ZctaCountyDocument)
            # doc_found: HospitalDischargeDocument
            if doc_found:
                doc_found.geocode_fips = doc_new.geocode_fips
                doc_found.geocode_fips_county = doc_new.geocode_fips_county
                doc_found.geocode_fips_state = doc_new.geocode_fips_state
                doc_found.percentage_zcta = doc_new.percentage_zcta
                doc_found.percentage_relationship = doc_new.percentage_relationship
                doc_found.save()
            else:
                doc_new.save()

        except Exception as e:
            errors.append(e)

        if errors:
            raise Exception(errors)

    def insert_census_geocode(self, document):
        errors = []
        try:
            doc_new = GeocodeDocument.from_json(json.dumps(document))
            # assert isinstance(doc_new, GeocodeDocument)
            doc_found = self.find_census_geocode(doc_new)
            # assert isinstance(doc_found, GeocodeDocument)
            if doc_found:
                doc_found.geocode_type = doc_new.geocode_type
                doc_found.geocode_state_fips = doc_new.geocode_state_fips
                doc_found.geocode_name = doc_new.geocode_name
                # doc_found.geocode_county = doc_new.geocode_county
                # doc_found.geocode_county_subdivision = doc_new.geocode_county_subdivision
                # doc_found.geocode_place = doc_new.geocode_place
                # doc_found.geocode_consolidated_city = doc_new.geocode_consolidated_city

                doc_found.save()
            else:
                doc_new.save()

        except Exception as e:
            errors.append(e)

        if errors:
            raise Exception(errors)

    def insert_census_shapefile(self, document):
        errors = []
        try:
            doc_new = ShapefileDocument.from_json(json.dumps(document))
            # assert isinstance(doc_new, GeocodeDocument)
            doc_found = self.find_census_shapefile(doc_new)
            # assert isinstance(doc_found, GeocodeDocument)
            if doc_found:
                doc_found.geocode_geometry = doc_new.geocode_geometry
                doc_found.save()
            else:
                doc_new.save()

        except Exception as e:
            errors.append(e)

        if errors:
            raise Exception(errors)

    def insert_inpatient_discharge(self, inpatient_discharge):
        doc_new = HospitalDischargeDocument.from_json(json.dumps(inpatient_discharge))
        facility = FacilityDocument.objects(facility_id_type=FacilityDocument.TYPE_CHHS,
                                            facility_id=doc_new.facility_id)
        if len(facility):
            facility = facility[0]
            doc_new.facility_zipcode = facility.zipcode

            facility_zcta = ZipcodeZctaDocument.objects(zipcode=facility.zipcode)

            if len(facility_zcta):
                facility_zcta = facility_zcta[0]
                doc_new.facility_zcta = facility_zcta.zcta

        patient_zcta = ZipcodeZctaDocument.objects(zipcode=doc_new.patient_zipcode)

        if len(patient_zcta):
            patient_zcta = patient_zcta[0]
            doc_new.patient_zcta = patient_zcta.zcta

        doc_new.save()
        # errors = []
        # try:
        #     doc_new = HospitalDischargeDocument.from_json(json.dumps(inpatient_discharge))
        #     doc_found = self.find_inpatient_discharge(doc_new)
        #     # doc_found: HospitalDischargeDocument
        #     if doc_found:
        #         doc_found.discharge_quantity = doc_new.discharge_quantity
        #         doc_found.save()
        #     else:
        #         facility = FacilityDocument.objects(facility_id_type=FacilityDocument.TYPE_CHHS,
        #                                             facility_id=doc_new.facility_id)
        #         if len(facility):
        #             facility = facility[0]
        #             doc_new.facility_zipcode = facility.zipcode
        #
        #             facility_zcta = ZipcodeZctaDocument.objects(zipcode=facility.zipcode)
        #
        #             if len(facility_zcta):
        #                 facility_zcta = facility_zcta[0]
        #                 doc_new.facility_zcta = facility_zcta.zcta
        #
        #         patient_zcta = ZipcodeZctaDocument.objects(zipcode=doc_new.patient_zipcode)
        #
        #         if len(patient_zcta):
        #             patient_zcta = patient_zcta[0]
        #             doc_new.patient_zcta = patient_zcta.zcta
        #
        #         doc_new.save()
        #
        # except Exception as e:
        #     errors.append(e)

        # if errors:
        #     raise Exception(errors)

    def insert_facility(self, dict_document):
        errors = []
        try:
            doc_new = FacilityDocument.from_json(json.dumps(dict_document))
            doc_found = self.find_facility(doc_new)
            if doc_found:
                doc_found.facility_name = doc_new.facility_name
                doc_found.facility_type = doc_new.facility_type
                doc_found.facility_subtype = doc_new.facility_subtype
                doc_found.address = doc_new.address
                doc_found.city = doc_new.city
                doc_found.state = doc_new.state
                doc_found.zipcode = doc_new.zipcode
                doc_found.county_name = doc_new.county_name
                doc_found.coordinates = doc_new.coordinates
                doc_found.save()
            else:
                doc_new.save()

        except Exception as e:
            errors.append(e)

        if errors:
            raise Exception(errors)

    def find_census_relationship_zcta_county(self, doc_found: ZctaCountyDocument):

        q = Q(geocode_fips=doc_found.geocode_fips,
              geocode_fips_county=doc_found.geocode_fips_county
              )
        doc_found = None
        try:
            doc_found = ZctaCountyDocument.objects.get(q)
        except Exception as e:
            pass

        return doc_found

    def find_census_geocode(self, document: GeocodeDocument):

        query = DAOMongo.get_query(document, [
            GeocodeDocument.geocode_type,
            GeocodeDocument.geocode_fips,
            GeocodeDocument.geocode_state_fips
        ])

        doc_found = None
        try:
            doc_found = GeocodeDocument.objects(query).first()
        except Exception as e:
            pass

        return doc_found

    def find_census_shapefile(self, document):

        q = Q(geocode_fips=document.geocode_fips)
        doc_found = None
        try:
            doc_found = ShapefileDocument.objects.get(q)
        except Exception as e:
            pass

        return doc_found

    def find_zip_zcta(self, doc_found):

        q = Q(zipcode=doc_found.zipcode,
              # zcta=doc_found.zcta,
              )
        doc_found = None
        try:
            doc_found = ZipcodeZctaDocument.objects.get(q)
        except Exception as e:
            pass

        return doc_found

    def insert_sca_bipartite(self, sca_bipartite_json):
        errors = []
        try:
            doc_new = NetworkHospitalDischargeDocument.from_json(sca_bipartite_json)
            doc_found = self.find_sca_bipartite(doc_new)
            # doc_found: HospitalDischargeDocument
            if doc_found:
                pass
            else:
                doc_new.save()
        except Exception as e:
            errors.append(e)

        if errors:
            raise Exception(errors)

    def insert_sca_bipartite_bulk(self, sca_bipartite_doc_iter):
        m = (pd.Series.to_dict(doc) for doc in sca_bipartite_doc_iter)
        try:
            NetworkHospitalDischargeDocument._get_collection().insert_many(m)
        except BulkWriteError as bwe:
            print(bwe.details)
            # you can also take this component and do more analysis
            # werrors = bwe.details['writeErrors']
            raise

    def find_sca_bipartite(self, doc_found):
        q = Q(facility_id=doc_found.facility_id,
              patient_zipcode=doc_found.patient_zipcode,
              discharge_quantity=doc_found.discharge_quantity,
              discharge_year=doc_found.discharge_year
              )
        doc_found = None
        try:
            doc_found = NetworkHospitalDischargeDocument.objects.get(q)
        except Exception as e:
            pass

        return doc_found

    def find_inpatient_discharge(self, doc_found):

        q = Q(facility_id=doc_found.facility_id,
              patient_zipcode=doc_found.patient_zipcode,
              discharge_type=doc_found.discharge_type,
              discharge_year=doc_found.discharge_year
              )
        doc_found = None
        try:
            doc_found = HospitalDischargeDocument.objects.get(q)
        except Exception as e:
            pass

        return doc_found

    def find_facility(self, doc_found):

        q = Q(facility_id=doc_found.facility_id,
              facility_id_type=doc_found.facility_id_type
              )
        doc_found = None
        try:
            doc_found = FacilityDocument.objects.get(q)
        except Exception as e:
            pass

        return doc_found

    @staticmethod
    def get_counts(field):
        field_discharge_year = HospitalDischargeDocument.discharge_year.db_field
        field_discharge_type = HospitalDischargeDocument.discharge_type.db_field
        field_discharge_quantity = HospitalDischargeDocument.discharge_quantity.db_field

        match = {}

        group = {}

        query = [

            {
                '$match':
                    {}
            },
            {
                '$group':
                    {
                        '_id': {
                            field: f'${field}',
                            field_discharge_year: f'${field_discharge_year}',
                            field_discharge_type: f'${field_discharge_type}',
                        },
                        field: {'$first': f'${field}'},
                        field_discharge_year: {'$first': f'${field_discharge_year}'},
                        field_discharge_type: {'$first': f'${field_discharge_type}'},
                        field_discharge_quantity: {'$sum': 1}

                    }
            },
            {
                '$group':
                    {
                        '_id': {
                            field_discharge_year: f'${field_discharge_year}',
                            field_discharge_type: f'${field_discharge_type}',
                        },
                        field_discharge_year: {'$first': f'${field_discharge_year}'},
                        field_discharge_type: {'$first': f'${field_discharge_type}'},
                        field_discharge_quantity: {'$sum': 1}
                    }
            },
            {
                '$project':
                    {
                        '_id': False,
                        field_discharge_year: True,
                        field_discharge_type: True,
                        field: f'${field_discharge_quantity}'
                    }
            },
            {
                '$sort':
                    {
                        field_discharge_year: 1,
                        field_discharge_type: 1
                    }
            }
        ]

        q = HospitalDischargeDocument.objects.aggregate(*query)
        df = pd.DataFrame.from_dict(q)
        df = df.set_index([field_discharge_year, field_discharge_type])
        return df

    @staticmethod
    def get_sum(field):
        field_discharge_year = HospitalDischargeDocument.discharge_year.db_field
        field_discharge_type = HospitalDischargeDocument.discharge_type.db_field
        field_discharge_quantity = HospitalDischargeDocument.discharge_quantity.db_field

        query = [

            {
                '$match':
                    {}
            },
            {
                '$group':
                    {
                        '_id': {
                            field_discharge_year: f'${field_discharge_year}',
                            field_discharge_type: f'${field_discharge_type}',
                        },
                        field_discharge_quantity: {'$sum': '$HPD_DISCHARGE_QUANTITY'}

                    }
            },
            {
                '$project':
                    {
                        '_id': False,
                        field_discharge_year: f'$_id.{field_discharge_year}',
                        field_discharge_type: f'$_id.{field_discharge_type}',
                        field_discharge_quantity: True
                    }
            },
            {
                '$sort':
                    {
                        field_discharge_year: 1,
                        field_discharge_type: 1
                    }
            }
        ]

        q = HospitalDischargeDocument.objects.aggregate(*query)
        df = pd.DataFrame.from_dict(q)
        df = df.set_index([field_discharge_year, field_discharge_type])
        return df


class ConfigDAO(object):

    def get_config_inpatient_discharge(self):
        query = Q(config_class="CHHS", config_name="PATIENT_ORIGIN_MARKET_SHARE")
        query_result = ConfigDocument.objects(query)
        results = ((item.config_key, item.config_value) for item in query_result)
        return iter(results)

    def get_config_census_shapefile(self):
        query = Q(config_class="CENSUS", config_name="SHAPEFILE")
        query_result = ConfigDocument.objects(query)
        results = ((item.config_key, item.config_value) for item in query_result)
        return iter(results)

    def get_config_census_geocodes(self):
        query = Q(config_class="CENSUS", config_name="GEOCODES")
        query_result = ConfigDocument.objects(query)
        results = ((item.config_key, item.config_value) for item in query_result)
        return iter(results)


d = DAOMongo()