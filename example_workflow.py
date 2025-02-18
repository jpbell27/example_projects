# Imports
import pandas as pd 
import numpy as np
import parsons
from parsons.google.google_bigquery import GoogleBigQuery
import textdistance
import os
import sys
import re
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
import json
import zipfile
import base64
import requests
from google.cloud import storage

# Set Google Cloud Platform (GCP) credentials (Redacted for privacy)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '<REDACTED>'

# Functions

def group_membership(input):
    '''Searches for a custom field ID and returns the associated text_value 
    for group role ("member", "leader", or None if no match is found).'''

    if input is None:
        return None
    
    # Search for custom_field_id and corresponding text_value
    match = re.search(r'custom_field_id":"<REDACTED>".*?text_value":"(.*?)"', input)

    if match:
        result = match.group(1).strip().lower()  # Capture and normalize text_value

        # Check for keywords in the result
        if "member" in result:
            return "member"
        elif "leader" in result:
            return "leader"
        else:
            return None

    return None  # Return None if no match is found

def assign_code(role):
    '''Assign an activist code based on parsed custom field responses.'''

    if role == "leader":
        return "<REDACTED>"  # Group leader ID
    elif role == "member":
        return "<REDACTED>"  # Group member ID
    else:
        return None  # NULL if neither role
    
def process_and_determine_role(df, col):
    '''Applies regex clean-up and transforms responses into activist codes.'''

    df['question_copy'] = df[col]  # Copy question column 
    df['question_copy'] = df['question_copy'].apply(group_membership)  # Regex clean-up
    df['role'] = df['question_copy'].apply(assign_code)  # Assign activist code

    return df

def group_name_parse(input):
    '''Extracts group name from a specific custom field response.'''

    if input is None:  
        return None

    match = re.search(r'"custom_field_id":"<REDACTED>".*?"text_value":"(.*?)"', input)

    if match:
        name = match.group(1)
        return name.lower()  # Convert to lowercase
    return None

def df_group_parser(df, col):
    '''Applies group name parser across the entire dataframe column.'''

    df['group'] = df[col]
    df['group'] = df['group'].apply(group_name_parse)  # Extract group names

    return df

def fuzzy_match(df1, df2, group_col, supportergroup_col, cutoff=0.8):
    '''Performs fuzzy matching to identify the best matches between groups.'''

    matches = []
    certainties = []

    for group in df1[group_col]:
        if group is None:
            matches.append(None)
            certainties.append(None)
            continue

        best_match = None
        best_score = 0
        
        for supportergroup in df2[supportergroup_col]:
            score = fuzz.WRatio(group, supportergroup) / 100.0
            if score > best_score:
                best_match = supportergroup
                best_score = score
        
        if best_score >= cutoff:
            matches.append(best_match)
            certainties.append(best_score)
        else:
            matches.append(None)
            certainties.append(None)

    df1['match'] = matches
    df1['certainty'] = certainties

    return df1

# BigQuery Setup (Project and Dataset info redacted)
project = '<REDACTED>'

# Instantiate Parsons BigQuery connector
bq = GoogleBigQuery(
    app_creds=os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
    project=project
)

# Supporter group data query
name_project = '<REDACTED>'
name_table = '<REDACTED>'
query = f"SELECT * FROM `{name_project}.{name_table}`"
name_parsons_table = bq.query(query)
names_df = name_parsons_table.to_dataframe()

# Event data query
organization_id = '<REDACTED>'
query = f"""
SELECT
  * 
FROM 
  `<REDACTED>`
WHERE 
  organization_id = {organization_id}
  AND utc_created_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
ORDER BY utc_created_date DESC;
"""
parsons_table = bq.query(query)
b_df = parsons_table.to_dataframe()

# Perform fuzzy matching
fuzzy_matches = fuzzy_match(b_df, names_df, 'group', 'supportergroupname', cutoff=0.9)

# Filter and clean data
fuzzy_matches_filtered = fuzzy_matches[(fuzzy_matches["role"].notna()) & (fuzzy_matches["match"].notna())]

# Upload to BigQuery
dataset = '<REDACTED>'
table = '<REDACTED-1'

if bq.table_exists(project, dataset, table):
    bq.upsert_data(project, dataset, table, fuzzy_matches_filtered)
else:
    bq.upload_data_if_not_exists(project, dataset, table, fuzzy_matches_filtered)

# Export to CSV
fuzzy_matches_filtered.to_csv('bulk_import.csv', index=False)

# Zip the file for upload
with zipfile.ZipFile("bulk_import.zip", "w") as zipf:
    zipf.write("bulk_import.csv")

# Upload to GCS
gcs_bucket_name = '<REDACTED>'  
gcs_blob_path = 'bulk_imports/bulk_import.zip'
storage.Client().bucket(gcs_bucket_name).blob(gcs_blob_path).upload_from_filename("bulk_import.zip")

# API authentication (Redacted)
username = os.getenv('EVERYACTION_API_USER', 'default_user')
apiKey = '<REDACTED>'
password = '<REDACTED>'
auth_string = f'{username}:{password}'
auth_encoded = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

headers = {
    'accept': 'application/json',
    'authorization': f'Basic {auth_encoded}'
}

# Payload for bulk import request
payload = {
    "file": {
        "fileName": "<REDACTED>",
        "hasHeader": True,
        "hasQuotes": False,
        "sourceUrl": "<REDACTED>",
        "columns": [
            {"name": "q1"},
            {"name": "q2"},
            {"name": "q3"},
            {"name": "q4"},
            {"name": "q5"},
            {"name": "q6"}
        ],
        "columnDelimiter": "csv"
    },
    "description": "Daily import matching individuals with groups and roles",
    "actions": [
        {
            "resultFileSizeKbLimit": 5000,
            "resourceType": "Contacts",
            "actionType": "loadMappedFile",
            "mappingTypes": [
                {"name": "CreateOrUpdateContact"},
                {"name": "ActivistCode", "fieldValueMappings": [{"fieldName": "Q1", "columnName": "q1"}]},
                {"name": "ApplyContactCustomFields", "fieldValueMappings": [{"fieldName": "Q2", "columnName": "q2"}]}
            ]
        }
    ]
}

# Make POST request to API (Redacted)
requests.post('<REDACTED>', headers=headers, json=payload)

response = requests.post('https://api.securevan.com/v4/bulkImportJobs', headers=headers, json=payload)
