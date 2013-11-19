import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

requirements = [req.strip() for req in open('requirements.pip')]
if sys.version_info[:2] <= (2, 6):
    requirements = [req.strip() for req in open('requirements_py26.pip')]

exec([v for v in open('kairos/__init__.py') if '__version__' in v][0])

setup(
    name='kairos',
    version=__version__,
    author='Aaron Westendorf',
    author_email="aaron@agoragames.com",
    packages = ['kairos'],
    install_requires = requirements,
    url='https://github.com/agoragames/kairos',
    license="LICENSE.txt",
    description='Time series data storage in Redis, Mongo, SQL and Cassandra',
    long_description=open('README.rst').read(),
    keywords=['python', 'redis', 'mongo', 'sql', 'mysql', 'sqlite', 'postgresql', 'cassandra', 'time', 'timeseries', 'rrd', 'gevent', 'statistics'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        "Intended Audience :: Developers",
        "Operating System :: POSIX",
        "Topic :: Communications",
        "Topic :: System :: Distributed Computing",
        "Topic :: Software Development :: Libraries :: Python Modules",
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries'
    ]
)
