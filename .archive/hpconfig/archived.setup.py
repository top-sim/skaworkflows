from setuptools import setup, find_packages

setup(
	name='hpconfig',
	version='0.1',
	packages=find_packages(exclude=["test"]),
	install_requires=[
	],
	url='https://github.com/top-sim/hpconfig',
	license='GNU',
	author='Ryan Bunney',
	author_email='ryan.bunney@icrar.org',
	description='hpconfig'
)
