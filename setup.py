import kairos
import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


requirements = map(str.strip, open('requirements.pip').readlines())

setup(
    name='kairos',
    version=kairos.VERSION,
    author='Aaron Westendorf',
    author_email="aaron@agoragames.com",
    packages = ['kairos'],
    install_requires = requirements,
    url='https://github.com/agoragames/kairos',
    license="LICENSE.txt",
    description='Time series data storage using Redis',
    long_description=open('README.rst').read(),
    keywords=['python', 'redis', 'time series', 'statistics'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        "Intended Audience :: Developers",
        "Operating System :: POSIX",
        "Topic :: Communications",
        "Topic :: System :: Distributed Computing",
        "Topic :: Software Development :: Libraries :: Python Modules",
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries'
    ]
)
