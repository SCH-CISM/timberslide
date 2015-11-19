try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'TimberSlide',
    'author': 'David F. Severski',
    'url': 'https://github.com/SCH-CISM/timberslide',
    'download_url': 'https://github.com/SCH-CISM/timberslide',
    'author_email': 'davidski@deadheaven.com',
    'version': '0.1',
    'install_requires': ['nose'],
    'packages': ['timberslide'],
    'scripts': ['bin/timberslide.py'],
    'name': 'timberslide',
    'test_suite': 'nose.collector',
    'tests_require': ['nose']
}

setup(**config)