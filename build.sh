#!/bin/bash

# This script is used to build the docker images for the spark and s3utils containers.

if [ ! -f scality-0.1-py3-none-any.whl ] ; then
    mkdir scality_tmp || exit 1
    cd scality_tmp || exit 1
    unzip ../scality-0.1-py2-none-any.whl
    # we don't want to load rpm installed scality modules
    mv scality scalityexport
    
    # Migrate from python2 to python3, 2to3 from python3-devel pkg.
    2to3 . -n -w .
    
  cat > setup.py << EOL
from setuptools import setup, find_packages

setup(
    name='scality',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        # List your package dependencies here
    ],
    entry_points={
        'console_scripts': [
            # Add any command-line scripts here
        ]
    },
    # Metadata
    author='CG',
    description='scality 2to3',
    license='Your License',
    keywords='some, keywords',
    url='http://your_package_url.com',
)
EOL
    
    # rebuild wheel
    python setup.py bdist_wheel
    
    cp dist/scality-0.1-py3-none-any.whl ../
    
    cd - || exit 1
fi

# https://github.com/sayedabdallah/Read-Write-AWS-S3
wget -N https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz
wget -N https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.770/aws-java-sdk-1.12.770.jar
wget -N https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# docker pull registry.scality.com/s3utils/s3utils:1.14.3
# docker save registry.scality.com/s3utils/s3utils:1.12.5 > s3utils_1.12.5.tar

docker build -f Dockerfile . -t registry.scality.com/spark/spark-container:3.5.2
# docker save registry.scality.com/spark/spark-container:3.5.2 > spark-container.tar

