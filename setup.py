from setuptools import setup
setup(
  name = 'redongo',
  packages = ['redongo'], # this must be the same as the name abovei
  version = '0.1.1',
  description = 'Gets stuff from a Redis queue and inserts it in Mongo',
  author = 'StoneWork Solutions',
  author_email = 'dev@stoneworksolutions.net',
  url = 'https://github.com/stoneworksolutions/redongo',
  download_url = 'https://github.com/stoneworksolutions/redongo/tarball/0.1.0',
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
