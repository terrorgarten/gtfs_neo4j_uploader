from setuptools import setup, find_packages

setup(
    name='gnuploader',
    version='1.0.0',
    packages=find_packages(),
    scripts=['uploader.py'],
    description='GTFS to Neo4j uploader package',
    author='Matěj Konopík',
    author_email='matejkonopik@gmail.com',
    url='https://github.com/terrorgarten/gnuploader',
    install_requires=[
        'certifi == 2023.7.22',
        'interchange == 2021.0.4',
        'monotonic == 1.6',
        'packaging == 23.2',
        'pansi == 2020.7.3',
        'py2neo == 2021.2.4',
        'Pygments == 2.16.1',
        'pytz == 2023.3.post1',
        'six == 1.16.0',
        'urllib3 == 2.0.7',
    ],
)
