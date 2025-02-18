Data Processing and Fuzzy Matching Pipeline

Overview

This project is a data ingestion, transformation, and matching pipeline built using Python, SQL, and Google Cloud Platform (GCP). The pipeline processes and integrates data from multiple sources, applies fuzzy string matching to standardize group names, and prepares the cleaned data for API ingestion. The system is designed to support large-scale electoral and advocacy campaigns by improving the accuracy of group affiliations.

Features

Data Extraction & Transformation: Pulls data from Google BigQuery and external sources.

Regex-based Data Cleaning: Extracts and standardizes key information from unstructured data fields.

Fuzzy Matching: Uses FuzzyWuzzy to match group names against a reference dataset with a customizable similarity threshold.

BigQuery Integration: Handles scalable database operations using Parsons' Google BigQuery connector.

Secure Data Handling: Ensures that credentials and sensitive data are managed securely.

Automated API Integration: Packages the cleaned data into a CSV, zips it, uploads it to Google Cloud Storage, and submits it to an external API for further processing.

Technologies Used

Python (pandas, numpy, re, fuzzywuzzy, requests, zipfile, base64, Google Cloud Storage)

SQL (Google BigQuery for structured data storage and querying)

Google Cloud Platform (GCP) (BigQuery, Cloud Storage)

Parsons Library (for seamless interaction with BigQuery)

REST API Calls (for data submission and integration with external systems)

Installation

Prerequisites

Ensure you have the following installed:

Python 3.7+

pip package manager

Google Cloud SDK

Required Python packages (install via pip install -r requirements.txt)

Environment Setup

Set up Google Cloud credentials:

export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"

Clone the repository and install dependencies:

git clone <repo_url>
cd <repo_name>
pip install -r requirements.txt

Usage

Running the Pipeline

Execute the main script:

python example_script.py

The script will:

Extract and clean data

Perform fuzzy matching

Upload cleaned data to BigQuery

Format and compress data for API submission

Submit data via API

Configuration

Modify the following parameters in the script as needed:

MATCH_THRESHOLD: Adjusts fuzzy matching sensitivity (default is 0.9 for high accuracy).

Database/Table Names: Update project and dataset references for your environment.

API Endpoints: Ensure the API URLs match your production or testing environment.

Security Considerations

Do not store credentials in the script; use environment variables.

Sensitive project identifiers have been removed in this public version.

Always validate input data to prevent injection attacks.

Future Improvements

Implement machine learning-based entity resolution for better matching accuracy.

Automate error detection and logging to improve data integrity.

Expand support for real-time data streaming instead of batch processing.

Author

Joseph Bell
Email: jpbell27@gmail.com


License

This project is for demonstration purposes only. Please consult with the author before commercial use.
