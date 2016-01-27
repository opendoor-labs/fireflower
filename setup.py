from setuptools import setup, find_packages
import subprocess

version_num = subprocess.check_output(['git', 'describe', '--tags']).decode('ascii').rstrip()

setup(
    name='fireflower',
    version=version_num,
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
