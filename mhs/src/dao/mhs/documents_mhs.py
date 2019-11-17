__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from datetime import datetime

from mongoengine import *
from mongoengine import signals

from mhs.src.dao.fields import DynamicGeoJsonBaseField
from mongoengine.fields import BaseField

DOC_MATCH = '$match'
DOC_PROJECT = '$project'


# class MHSBaseEmbeddedDocument(EmbeddedDocument):
#     meta = {
#         'allow_inheritance': True,
#         'abstract': True
#     }


class MHSBaseDocument(Document):
    meta = {
        'allow_inheritance': True,
        'abstract': True,
        'index_cls': False,
    }

    def get_data(self):
        return self._data


class MHSBaseDocumentAudit(MHSBaseDocument):
    meta = {
        'allow_inheritance': True,
        'abstract': True,
        'index_cls': False,
    }
    created = DateTimeField(required=True, default=datetime.now)
    updated = DateTimeField(required=True, default=datetime.now)

    @classmethod
    def post_init(cls, sender, document, **kwargs):
        document.created = datetime.now()

    @classmethod
    def pre_save(cls, sender, document, **kwargs):
        document.updated = datetime.now()


class ConfigDocument(MHSBaseDocument):
    meta = {
        'collection': 'CFG_CONFIG',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'config_class',
                            'config_name',
                            'config_key',
                        ],
                    'unique': True,
                    'name': f'CFG_CONFIG_INDEX'
                }
            ]

    }
    config_key = StringField(required=True, db_field='CFG_KEY')
    config_name = StringField(required=True, db_field='CFG_NAME')
    config_class = StringField(required=True, db_field='CFG_CLASS')
    config_value = StringField(required=True, db_field='CFG_VALUE')


class GeocodeDocument(MHSBaseDocument):
    meta = {
        'collection': 'GEO_GEOCODES',
        'allow_inheritance': True,
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'geocode_fips',
                            'geocode_type',
                            'geocode_state_fips'
                        ],
                    'unique': True,
                    'name': f'GEO_GEOCODES_INDEX'
                }
            ]
        # 'abstract': True
    }
    # TYPE_COUNTRY = 'COUNTRY'
    TYPE_STATE = '040'
    TYPE_COUNTY = '050'
    TYPE_COUNTY_SUBDIVISION = '061'
    TYPE_PLACE = '162'
    TYPE_CONSOLIDATED_CITY = '170'

    # TYPE_ZCTA = 'ZCTA'

    TYPES = [TYPE_STATE, TYPE_COUNTY, TYPE_COUNTY_SUBDIVISION, TYPE_PLACE, TYPE_CONSOLIDATED_CITY]

    geocode_fips = StringField(db_field='GEO_FIPS', required=True)
    geocode_type = StringField(db_field='GEO_TYPE', required=True)
    geocode_state_fips = StringField(db_field='GEO_STATE_FIPS', required=True)
    geocode_name = StringField(db_field='GEO_NAME', required=True)

    # geocode_county = StringField(db_field='GEO_COUNTY_FIPS', required=False)
    # geocode_county_subdivision = StringField(db_field='GEO_COUNTY_SUBDIVISION_FIPS', required=False)
    # geocode_place = StringField(db_field='GEO_PLACE_FIPS', required=False)
    # geocode_consolidated_city = StringField(db_field='GEO_CONSOLIDATE_CITY_FIPS', required=False)

    def validate(self, clean=True):
        return self.geocode_type in GeocodeDocument.TYPES


class ZctaCountyDocument(MHSBaseDocument):
    meta = {
        'collection': 'RZC_ZCTA_COUNTY',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'geocode_fips',
                            'geocode_fips_county',
                        ],
                    'unique': True,
                    'name': f'RZC_ZCTA_COUNTY_INDEX'
                }
            ]
    }

    geocode_fips = StringField(db_field='GEO_FIPS', required=True)
    geocode_fips_county = StringField(db_field='GEO_FIPS_COUNTY', required=True)
    geocode_fips_state = StringField(db_field='GEO_FIPS_STATE', required=True)
    percentage_zcta = FloatField(db_field='RZC_PERCENTAGE_ZCTA', required=True)
    percentage_relationship = FloatField(db_field='RZC_PERCENTAGE_RELATIONSHIP')

    @classmethod
    def post_init(cls, sender, document, **kwargs):
        cls.geocode_fips.max_length = 5
        cls.geocode_fips.min_length = 5
        super(ZctaCountyDocument, cls).post_init(sender, document, **kwargs)

    @classmethod
    def pre_save(cls, sender, document, **kwargs):
        # cls.geocode_type.default = 'zcta'
        document.geocode_type = GeocodeDocument.TYPE_ZCTA
        super(ZctaCountyDocument, cls).pre_save(sender, document, **kwargs)


class ShapefileDocument(MHSBaseDocument):
    meta = {
        'allow_inheritance': True,
        'collection': 'SHP_SHAPEFILE',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'geocode_fips',
                        ],
                    'unique': True,
                    'name': f'RZC_ZCTA_COUNTY_INDEX'

                }
            ]
        # 'abstract': True
    }
    geocode_fips = StringField(db_field='SHP_FIPS', required=True)
    geocode_type = StringField(db_field='SHP_TYPE', required=True)
    geocode_geometry = DynamicGeoJsonBaseField(db_field='SHP_GEOMETRY', required=True)


# class CountyDocument(GeocodeDocument):
#     meta = {'collection': 'PLC_ZCTA'}
#
#     @classmethod
#     def pre_save(cls, sender, document, **kwargs):
#         # cls.geocode_type.default = 'county'
#         document.geocode_type = 'county'
#         super(CountyDocument, cls).pre_save(sender, document, **kwargs)

class ZipcodeZctaDocument(MHSBaseDocument):
    meta = {
        'collection': 'ZZA_ZIPCODE_ZCTA',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'zipcode',
                            'zcta',
                        ],
                    'unique': True,
                    'name': f'ZZA_ZIPCODE_ZCTA_INDEX'

                }
            ]
    }

    zipcode = StringField(db_field='ZZA_ZIPCODE', required=True, unique=True, min_length=5, max_length=5)
    zcta = StringField(db_field='ZZA_ZCTA', required=True, min_length=5, max_length=5)
    state = StringField(db_field='ZZA_STATE', required=True, max_length=2)
    city = StringField(db_field='ZZA_CITY', required=True)


class FacilityDocument(MHSBaseDocument):
    TYPE_CMS = 'CMS'
    TYPE_CHHS = 'CHHS'
    TYPES = [TYPE_CHHS, TYPE_CMS]
    meta = {
        'allow_inheritance': True,
        'abstract': False,
        'collection': 'FAC_FACILITY',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'facility_id',
                            'facility_id_type'
                        ],
                    'unique': True,
                    'name': f'FAC_FACILITY_INDEX'

                }
            ]
    }
    facility_id = StringField(db_field="FAC_ID")
    facility_id_type = StringField(db_field="FAC_ID_TYPE")
    facility_name = StringField(db_field="FAC_NAME")
    facility_type = StringField(db_field='FAC_TYPE', null=True)
    facility_subtype = StringField(db_field='FAC_SUBTYPE', null=True)
    address = StringField(db_field='FAC_ADDRESS')
    city = StringField(db_field='FAC_CITY')
    state = StringField(db_field='FAC_STATE')
    zipcode = StringField(db_field='FAC_ZIPCODE')
    county_name = StringField(db_field='FAC_COUNTY')
    coordinates = PointField(db_field='FAC_COORDINATES')
    # hospital_type = StringField(db_field='HOS_TYPE')


class SharedCareArea(MHSBaseDocument):
    meta = {
        'allow_inheritance': True,
        'abstract': False,
        'collection': 'SCA_SHARED_CARE_AREA',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'method',
                            'year',
                            'type_discharge',
                            'type_community_detection',
                            'zcta',
                            'sca_id'
                        ],
                    'unique': True,
                    'name': f'SCA_SHARED_CARE_AREA_INDEX'
                }
            ]
    }

    zcta = StringField(db_field='SCA_ZCTA', required=True, min_length=5, max_length=5)
    sca_id = StringField(db_field='SCA_ID', required=True)
    method = StringField(db_field='SCA_METHOD', required=True)
    year = IntField(db_field='SCA_YEAR', required=True)
    type_discharge = StringField(db_field='SCA_TYPE_DISCHARGE', required=True)
    type_community_detection = StringField(db_field='SCA_TYPE_COMMUNITY_DETECTION', required=True)


class NetworkMeasuresDocument(MHSBaseDocument):
    meta = {
        'allow_inheritance': True,
        'abstract': False,
        'collection': 'NTM_NETWORK_MEASURES',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'metric',
                            'method',
                            'year',
                            'type_discharge',
                            'normalized',
                        ],
                    'unique': False,
                    'name': f'NTM_NETWORK_MEASURES_INDEX'
                }
            ]
    }

    metric = StringField(db_field='NTM_METRIC', required=True)
    metric_value = FloatField(db_field='NTM_METRIC_VALUE', required=True)
    method = StringField(db_field='NTM_SCA_METHOD', required=True)
    year = IntField(db_field='NTM_SCA_YEAR', required=True)
    type_discharge = StringField(db_field='NTM_SCA_TYPE_DISCHARGE', required=True)
    normalized = BooleanField(db_field='NTM_SCA_NORMALIZED', required=True)


class MetricsNetworkSharedCareAreaDocument(MHSBaseDocument):
    meta = {
        'allow_inheritance': True,
        'abstract': False,
        'collection': 'MNS_METRICS_NETWORK_SHARED_CARE_AREA',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'metric',
                            'method',
                            'year',
                            'type_discharge'
                        ],
                    'unique': False,
                    'name': f'MNS_METRICS_NETWORK_SHARED_CARE_AREA_INDEX'
                }
            ]
    }

    metric = StringField(db_field='MNS_METRIC', required=True)
    metric_value = FloatField(db_field='MNS_METRIC_VALUE', required=True)
    sca_id = StringField(db_field='MNS_SCA_ID', required=True)
    method = StringField(db_field='MNS_SCA_METHOD', required=True)
    year = IntField(db_field='MNS_SCA_YEAR', required=True)
    type_discharge = StringField(db_field='MNS_SCA_TYPE_DISCHARGE', required=True)


class MetricsSharedCareAreaDocument(MHSBaseDocument):
    meta = {
        'allow_inheritance': True,
        'abstract': False,
        'collection': 'MSA_METRICS_SHARED_CARE_AREA',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'metric',
                            'method',
                            'year',
                            'type_discharge',
                            'type_community_detection',
                            'normalized',
                            'sca_id'
                        ],
                    'unique': True,
                    'name': f'MSA_METRICS_SHARED_CARE_AREA_INDEX'
                }
            ]
    }

    metric = StringField(db_field='MSA_METRIC', required=True)
    metric_value = FloatField(db_field='MSA_METRIC_VALUE', required=True)
    sca_id = StringField(db_field='MSA_SCA_ID', required=True)
    method = StringField(db_field='MSA_SCA_METHOD', required=True)
    year = IntField(db_field='MSA_SCA_YEAR', required=True)
    type_discharge = StringField(db_field='MSA_SCA_TYPE_DISCHARGE', required=True)
    type_community_detection = StringField(db_field='MSA_SCA_TYPE_COMMUNITY_DETECTION', required=True)
    normalized = BooleanField(db_field='MSA_SCA_NORMALIZED', required=True)


class HospitalDischargeDocument(MHSBaseDocument):
    meta = {
        'collection': 'HPD_HOSPITAL_DISCHARGE',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'facility_id',
                            'facility_county',
                            'patient_zipcode',
                            'patient_county',
                            'discharge_type',
                            'discharge_year'
                        ],
                    'unique': True,
                    'name': f'HPD_HOSPITAL_DISCHARGE_INDEX'
                }
            ]
    }

    TYPE_DISCHARGE_ED_ONLY = 'ED Only'
    TYPE_DISCHARGE_INPANTIENT = 'Inpatient'
    TYPE_DISCHARGE_INPATIENT_FROM_ED = 'Inpatient from ED'
    TYPE_DISCHARGE_AS_ONLY = 'AS Only'
    TYPES_DISCHARGE = [TYPE_DISCHARGE_AS_ONLY,
                       TYPE_DISCHARGE_ED_ONLY,
                       TYPE_DISCHARGE_INPANTIENT,
                       TYPE_DISCHARGE_INPATIENT_FROM_ED]

    facility_id = StringField(db_field='HPD_FACILITY_ID', required=True)
    facility_name = StringField(db_field='HPD_FACILITY_NAME', required=True)
    facility_zipcode = StringField(db_field='HPD_FACILITY_ZIPCODE', required=False)
    facility_zcta = StringField(db_field='HPD_FACILITY_ZCTA', required=False)
    facility_county = StringField(db_field='HPD_FACILITY_COUNTY')
    patient_county = StringField(db_field='HPD_PATIENT_COUNTY')
    patient_zipcode = StringField(db_field='HPD_PATIENT_ZIPCODE', required=False)
    patient_zcta = StringField(db_field='HPD_PATIENT_ZCTA', required=False)
    discharge_year = IntField(db_field='HPD_DISCHARGE_YEAR', required=True)
    discharge_type = StringField(db_field='HPD_DISCHARGE_TYPE', required=True)
    discharge_quantity = IntField(db_field='HPD_DISCHARGE_QUANTITY', required=True)


class NetworkHospitalDischargeDocument(MHSBaseDocument):
    meta = {
        'collection': 'NHD_NETWORK_HOSPITAL_DISCHARGE',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'network_method',
                            'network_year',
                            'network_type',
                            'node_in',
                            'node_out'
                        ],
                    'unique': True,
                    'name': 'NHD_NETWORK_HOSPITAL_DISCHARGE_INDEX'
                }
            ]
    }
    network_method = StringField(db_field='NHD_NETWORK_METHOD', required=True)
    network_year = IntField(db_field='NHD_DISCHARGE_YEAR', required=True)
    network_type = StringField(db_field='NHD_DISCHARGE_TYPE', required=True)
    node_in = StringField(db_field='NHD_NODE_IN')
    node_out = StringField(db_field='NHD_NODE_OUT')
    weight = FloatField(db_field='NHD_WEIGHT')


class PartDMedicareProviderDocument(MHSBaseDocument):
    meta = {
        'collection': 'MPP_PARTD_PRESCRIBER',
        'index_cls': False,
        'indexes':
            [
                {
                    'fields':
                        [
                            'provider_npi',
                            'drug_name_brand',
                            'drug_name_generic'
                        ],
                    'unique': True,
                    'name': 'MPP_PARTD_PRESCRIBER_INDEX'
                }
            ]
    }
    provider_npi = StringField(db_field='MPP_NPI', required=True)
    provider_name_first = StringField(db_field='MPP_PROVIDER_NAME_FIRST', required=True)
    provider_name_last = StringField(db_field='MPP_PROVIDER_NAME_LAST', required=True)
    provider_specialty = StringField(db_field='MPP_PROVIDER_SPECIALTY', required=True)
    provider_city = StringField(db_field='MPP_PROVIDER_CITY', required=True)
    provider_state = StringField(db_field='MPP_PROVIDER_STATE', required=True)
    # 'nppes_provider_state',
    drug_name_brand = StringField(db_field='MPP_DRUG_BRAND_NAME', required=True)
    drug_name_generic = StringField(db_field='MPP_DRUG_GENERIC_NAME', required=True)
    beneficiary_count = IntField(db_field='MPP_BENEFICIARY_COUNT', required=False)
    total_claim_count = IntField(db_field='MPP_CLAIM_TOTAL_COUNT', required=True)
    total_30_day_fill_count = FloatField(db_field='MPP_CLAIM_TOTAL_30DAY_FILL_COUNT', required=True)
    total_day_supply = IntField(db_field='MPP_CLAIM_TOTAL_DAY_SUPPLY', required=False)
    total_drug_cost = FloatField(db_field='MPP_CLAIM_TOTAL_DRUG_COST', required=False)
