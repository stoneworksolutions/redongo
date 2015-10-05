from setuptools import setup, find_packages

DIRS_EXCLUDED = ['dist', 'build', 'docs', 'tests']

setup(
  name = 'redongo',
  packages = find_packages(exclude=DIRS_EXCLUDED),
  version = '0.1.10.2',
  description = 'Gets stuff from a Redis queue and inserts it in Mongo',
  author = 'StoneWork Solutions',
  author_email = 'dev@stoneworksolutions.net',
  url = 'https://github.com/stoneworksolutions/redongo',
  download_url = 'https://github.com/stoneworksolutions/redongo/tarball/0.1.10',
  keywords = ['redis', 'mongo', 'bulks'], # arbitrary keywords
  classifiers = [],
  install_requires = [
	'pycrypto',
	'pymongo',
	'redis',
	'ujson',
	'Twisted',
  ]
)
