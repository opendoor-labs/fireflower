from setuptools import setup, find_packages

setup(
    name='fireflower',
    version='1.0.3',
    description='Enhancements for Luigi task management',
    url='https://github.com/opendoor-labs/fireflower',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=[
        'arrow',
        'boto',
        'luigi',
        'pandas',
        'sqlalchemy',
        'toolz'
    ]
)
