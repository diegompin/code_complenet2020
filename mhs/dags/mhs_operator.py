__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'

from airflow.operators import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging


class MHSOperator(object):

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._log = logging.getLogger(__name__)
        super().__init__()

    @property
    def log(self):
        return self._log

    def create_operator(self, *args, **kwargs):
        operator_cls = self.get_operator_class()
        operator_obj = operator_cls(**kwargs)
        return operator_obj

    @classmethod
    def get_operator_class(cls):
        raise NotImplemented('get_operator')


class MHSPythonOperator(MHSOperator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def get_operator_class(cls):
        return PythonOperator

    def get_script(self):
        raise NotImplemented('python_callable not implemented')

    def create_operator(self, *args, **kwargs):
        # task_id = kwargs['task_id']
        python_callable = kwargs['python_callable']
        kwargs.update({
            'task_id': f'{python_callable.name}'
        })
        return super().create_operator(*args, **kwargs)


class MHSPythonAcquisitionOperator(MHSOperator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def get_operator_class(cls):
        return PythonOperator

    def get_script(self):
        raise NotImplemented('python_callable not implemented')

    def create_operator(self, *args, **kwargs):
        # task_id = kwargs['task_id']
        python_callable = kwargs['python_callable']
        config_class = kwargs['op_kwargs']['config_class']
        config_name = kwargs['op_kwargs']['config_name']
        config_key = kwargs['op_kwargs']['config_key']
        kwargs.update({
            'task_id': f'{python_callable.name}_{config_class}_{config_name}_{config_key}'
        })
        return super().create_operator(*args, **kwargs)
