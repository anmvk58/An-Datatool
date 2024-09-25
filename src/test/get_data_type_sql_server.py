import sys
import logging
import pymssql
import json
import os
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

def lambda_handler(event, context):

    return {
        'pymssql': pymssql.__version__
    }

