# Boto-Plus
An addition to [Python's boto3 library](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) that makes it easier to interact with the boto3 API. This library also introduces more complicated functionality, such as a Python-based replication of the great [AWS S3 cli command](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/sync.html) `aws s3 sync`.


# Requirements
- Python >= 3.9 <br> 

| Package | Version |
| ------- | ------- |
| boto3 | 1.34 |
| botocore | 1.34 |
| MarkupSafe | 2.1.1 |
| more-itertools | 10.2 |
| werkzeug | 3.0 |
| moto | 5.0 |


# Installation
```
# create the environment (either with venv or conda)
python3 -m venv boto_plus
conda create --name boto_plus python=3.9

# activate the environment (either with venv or conda)
source boto_plus/bin/activate
conda activate boto_plus

# clone the repository
git clone git@github.com:andrewstenger/boto-plus.git

# install the requirements
cd boto-plus
pip3 install --upgrade pip
pip3 install -U -r requirements.txt

# install boto-plus
pip3 install .
```

# Unit Testing
A suite of unit tests have been supplied. These tests implement [Python's unittest library](https://docs.python.org/3/library/unittest.html). <br>
The unit tests can be run using the following command:
```
python3 -m unittest discover ./tests
```
