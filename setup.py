import re

from setuptools import find_packages
from setuptools import setup


def load_reqs(filename):
    with open(filename) as reqs_file:
        return [
            re.sub('==', '>=', line) for line in reqs_file.readlines()
            if not re.match('\s*#', line)
        ]


requirements = load_reqs('requirements.txt')
test_requirements = load_reqs('test-requirements.txt')

setup(
    name='aiogrpc_etcd3',
    version='0.1.3',
    description='AsyncIO ETCD3 GRPC driver',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    url='http://github.com/onna/aiogrpc_etcd3',
    author='Ramon Navarro',
    author_email='ramon@onna.com',
    license='MIT',
    packages=find_packages(),
    zip_safe=False,
    install_requires=requirements,
    test_suite='tests',
    tests_require=test_requirements
)
