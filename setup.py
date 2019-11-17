from setuptools import setup
import os


def package_files(directory):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join('..', path, filename))
    return paths

extra_files = package_files('mhs')

setup(
    name='mhs',
    version='1.0.0',
    packages=['mhs'],
    package_data={'': extra_files},
    url='',
    license='',
    author='Diego Pinheiro',
    author_email='',
    description='Mapping the Health System'
)



#
# setup(
#     name='mhs',
#     version='1.0.0',
#     packages=['mhs'],
#     url='',
#     license='',
#     author='Diego Pinheiro',
#     author_email='',
#     description=''
# )


