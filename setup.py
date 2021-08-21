#!/usr/bin/env python

import versioneer
from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

with open('requirements.in') as requirements_file:
    requirements = requirements_file.read().split()

test_requirements = ['pytest>=3', ]

setup(
    author='Andoni Sooklaris',
    author_email='andoni.sooklaris@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description='Extract local iMessage data to a Postgres database or flat textfiles',
    entry_points={
        'console_scripts': [
            'imessage-extractor=imessage_extractor.cli:main',
        ],
    },
    install_requires=requirements,
    license='MIT license',
    # long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='imessage_extractor',
    name='imessage_extractor',
    packages=find_packages(include=['imessage_extractor', 'imessage_extractor.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/tsouchlarakis/imessage_extractor',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    zip_safe=False,
)
