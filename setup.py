from setuptools import setup, find_packages

setup(
    name='es-ingester',
    version='0.1.0',
    author='Siva Krishna',
    author_email='your.email@example.com',  # Replace with your email
    description='A module for ingesting data into Elasticsearch',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/es-ingester',  # Update with your repository URL
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',  # Update with your license
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',  # Minimum Python version requirement
    install_requires=[
        'elasticsearch>=7.0.0',  # Dependency for Elasticsearch
        'jsonlines',               # For handling JSONL files
        'pyyaml',                  # Optional: For YAML config file support
    ],
    entry_points={
        'console_scripts': [
            'es_ingester=es_ingester.core:main',  # Add this line for CLI
        ],
    },
)
