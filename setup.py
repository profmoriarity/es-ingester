from setuptools import setup, find_packages

setup(
    name='es-ingester',
    version='0.1.1',
    author='Siva Krishna',
    author_email='krishna.krish759213@gmail.com',
    description='A module for ingesting data into Elasticsearch',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/profmoriarity/es-ingester', 
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License', 
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    install_requires=[
        'elasticsearch>=7.0.0', 
        'jsonlines',
        'pyyaml', 
    ],
    entry_points={
        'console_scripts': [
            'es_ingester=es_ingester.core:main', 
        ],
    },
)
