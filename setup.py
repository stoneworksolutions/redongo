from setuptools import setup, find_packages
import pip
import platform

DIRS_EXCLUDED = ['dist', 'build', 'docs', 'tests']

install_reqs = pip.req.parse_requirements('requirements.txt', session=pip.download.PipSession())
# install_requires is a list of requirements
install_requires = [str(ir.req) for ir in install_reqs]

if platform.python_implementation() != 'PyPy':
    install_requires.append('ujson==1.30')

setup(
    name='redongo',
    packages=find_packages(exclude=DIRS_EXCLUDED),
    version='0.2.4',
    description='Gets stuff from a Redis queue and inserts it in Mongo',
    author='StoneWork Solutions',
    author_email='dev@stoneworksolutions.net',
    url='https://github.com/stoneworksolutions/redongo',
    download_url='https://github.com/stoneworksolutions/redongo/tarball/0.2.4',
    keywords=['redis', 'mongo', 'bulks'],  # arbitrary keywords
    classifiers=[],
    install_requires=install_requires,
)
