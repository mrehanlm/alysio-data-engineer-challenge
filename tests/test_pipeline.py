import os

import pandas as pd
import pytest

from etl.pipeline import Pipeline
from models import Company, ContactStatus, ForecastCategory, Industry, Product, Stage
from utils.db import BaseModel
from utils.db import db_session as mock_db_session
from utils.db import engine as mock_engine

errors_path = "tests/data/errors"


@pytest.fixture
def db_session():
    """Set up the database schema and provide a session."""
    BaseModel.metadata.create_all(bind=mock_engine)
    with mock_db_session() as session:
        yield session

    BaseModel.metadata.drop_all(bind=mock_engine)


@pytest.fixture
def mock_data_files():
    """Fixture to create temporary CSV files with mock data."""
    files = {}
    tmpdir = "tests/data"

    # Prepare mock data for each CSV
    companies_data = [
        {
            "id": "COM12312",
            "name": "Company 1",
            "domain": "company1.biz",
            "industry": "Technology",
            "size": "201-500",
            "country": "US",
            "created_date": "2024-06-15T08:45:00",
            "is_customer": True,
            "annual_revenue": 1500000,
        },
        {
            "id": "COM12112",
            "name": "Company 2",
            "domain": "company2.net",
            "industry": "Healthcare",
            "size": "201-500",
            "country": "UK",
            "created_date": "2023-09-10T12:30:00",
            "is_customer": False,
            "annual_revenue": 3000000,
        },
    ]
    contacts_data = [
        {
            "id": "CONT1",
            "email": "first0.last0@company1.com",
            "first_name": "First0",
            "last_name": "Last0",
            "title": "CEO",
            "company_id": 1,
            "phone": "+1-555-123-4567",
            "status": "Lead",
            "created_date": "2024-06-15T08:45:00",
            "last_modified": "2025-01-15T14:20:00",
        },
        {
            "id": "CONT2",
            "email": "first1.last1@company2.com",
            "first_name": "First1",
            "last_name": "Last1",
            "title": "Manager",
            "company_id": 2,
            "phone": "+1-555-234-5678",
            "status": "Customer",
            "created_date": "2023-09-10T12:30:00",
            "last_modified": "2025-01-19T09:00:00",
        },
    ]
    opportunities_data = [
        {
            "id": "OP1",
            "name": "Company 1 - Basic Deal",
            "contact_id": 1,
            "company_id": 1,
            "amount": 25000,
            "stage": "Prospecting",
            "product": "Basic",
            "probability": 20,
            "created_date": "2024-06-15T08:45:00",
            "close_date": "2024-09-15T08:45:00",
            "is_closed": False,
            "forecast_category": "Pipeline",
        },
        {
            "id": "OP2",
            "name": "Company 2 - Pro Deal",
            "contact_id": 2,
            "company_id": 2,
            "amount": 45000,
            "stage": "Negotiation",
            "product": "Pro",
            "probability": 50,
            "created_date": "2023-09-10T12:30:00",
            "close_date": "2024-02-10T12:30:00",
            "is_closed": False,
            "forecast_category": "Best Case",
        },
    ]
    activities_data = [
        {
            "id": "ACTIVITY1",
            "type": "Call",
            "contact_id": 1,
            "opportunity_id": 1,
            "subject": "Call with First0",
            "duration_minutes": 30,
            "outcome": "Completed",
            "notes": "Discussed the project scope",
            "timestamp": "2024-06-15T08:45:00",
        },
        {
            "id": "ACTIVITY2",
            "type": "Email",
            "contact_id": 2,
            "opportunity_id": 2,
            "subject": "Email with First1",
            "duration_minutes": 15,
            "outcome": "Rescheduled",
            "notes": "Requested a follow-up meeting",
            "timestamp": "2024-06-15T08:45:00",
        },
    ]

    # Create temporary CSV files for each dataset
    companies_file = os.path.join(tmpdir, "companies.csv")
    pd.DataFrame(companies_data).to_csv(companies_file, index=False)
    files["companies"] = str(companies_file)

    contacts_file = os.path.join(tmpdir, "contacts.json")
    pd.DataFrame(contacts_data).to_json(contacts_file, index=False)
    files["contacts"] = str(contacts_file)

    opportunities_file = os.path.join(tmpdir, "opportunities.csv")
    pd.DataFrame(opportunities_data).to_csv(opportunities_file, index=False)
    files["opportunities"] = str(opportunities_file)

    activities_file = os.path.join(tmpdir, "activities.json")
    df = pd.DataFrame(activities_data)
    df["timestamp"] = df["timestamp"].astype(str)
    df.to_json(activities_file, index=False)
    files["activities"] = str(activities_file)

    return files


def add_record(new_record, file_name):
    """
    Add a new record to the mock data file.
    """
    extension = file_name.split(".")[1]
    file = os.path.join("tests/data", file_name)
    if extension == "csv":
        df = pd.read_csv(file)
    else:
        df = pd.read_json(file)

    # Append the new record to the DataFrame
    modified_df = df._append(new_record, ignore_index=True)

    if extension == "csv":
        modified_df.to_csv(file, index=False)
    else:
        modified_df.to_json(file, index=False)


class TestPipeline:
    """
    Test cases for the ETL pipeline.
    """

    def test_pipeline_incremental_update(self, db_session, mock_data_files):
        """
        Test the incremental update behavior of ETL pipeline using mock CSV files.
        """
        # Initialize the pipeline with the mock data path
        pipeline = Pipeline(path="tests/data", errors_path=errors_path)

        # First run: Process all data
        pipeline.run()

        # Check the number of companies in the database after the first run
        companies = db_session.query(Company).all()
        assert len(companies) == 2  # Two companies should have been added

        # Modify Companies
        new_company = {
            "id": "COM99999",
            "name": "Company 3",
            "domain": "company3.com",
            "industry": "Finance",
            "size": "201-500",
            "country": "US",
            "created_date": "2025-01-20T10:00:00",
            "is_customer": False,
            "annual_revenue": 5000000,
        }
        add_record(new_company, "companies.csv")

        # Second run: Process the same data (incremental update)
        pipeline.run()

        # Check the number of companies again after the second run
        companies = db_session.query(Company).all()
        assert len(companies) == 3  # No new companies should be added

        # Check that the validation errors list is empty after both runs
        assert not pipeline.validation_errors["companies"]
        assert not pipeline.validation_errors["contacts"]
        assert not pipeline.validation_errors["opportunities"]
        assert not pipeline.validation_errors["activities"]

    def test_validation_errors(self, db_session, mock_data_files):
        """
        Test validation errors handling in ETL pipeline using mock CSV files.
        """

        pipeline = Pipeline(path="tests/data", errors_path=errors_path)

        # Add a invalid record company
        new_company = {
            "id": "COM99999",
            "name": "Company 3",
            "domain": "company3.com",
            "industry": "Finance",
            "size": "201-500",
            "country": "US",
            "created_date": "9999978999999",
            "is_customer": False,
            "annual_revenue": 1000000,
        }
        add_record(new_company, "companies.csv")

        new_contact = {
            "id": "CONT431",
            "email": "invalid_email",
            "first_name": "First0",
            "last_name": "Last0",
            "title": "CEO",
            "company_id": 1,
            "phone": "11231231234",
            "status": "Lead",
            "created_date": "2024-06-15T08:45:00",
            "last_modified": "2025-01-15T14:20:00",
        }
        add_record(new_contact, "contacts.json")

        # Add a invalid record opportunity
        new_opportunity = {
            "id": "OP11111",
            "name": "Company 1 - Basic Deal",
            "contact_id": 1,
            "company_id": 1,
            "amount": 25000,
            "stage": "Prospecting",
            "product": "Basic",
            "probability": -20,
            "created_date": "2024-06-15T08:45:00",
            "close_date": "2024-09-15T08:45:00",
            "is_closed": False,
            "forecast_category": "Pipeline",
        }
        add_record(new_opportunity, "opportunities.csv")

        pipeline.run()

        industry = db_session.query(Industry).filter_by(name="FINANCE").one()
        expected_response = [
            {
                "record": {
                    "id": "COM99999",
                    "name": "Company 3",
                    "domain": "company3.com",
                    "size": "201-500",
                    "country": "US",
                    "created_date": "9999978999999",
                    "is_customer": False,
                    "annual_revenue": 1000000,
                    "industry_id": industry.id,
                },
                "errors": [
                    "created_date: 9999978999999 is invalid. Please consider ISO 8601 format."
                ],
            }
        ]
        assert pipeline.validation_errors["companies"] == expected_response

        status = db_session.query(ContactStatus).filter_by(name="LEAD").one()
        expected_response = [
            {
                "record": {
                    "id": "CONT431",
                    "email": "invalid_email",
                    "first_name": "First0",
                    "last_name": "Last0",
                    "title": "CEO",
                    "company_id": 1,
                    "phone": "11231231234",
                    "created_date": "2024-06-15T08:45:00",
                    "last_modified": "2025-01-15T14:20:00",
                    "status_id": status.id,
                },
                "errors": [
                    "email: invalid_email is not a valid email address.",
                    "phone: 11231231234 is not a valid. Please provide a valid phone number in international format.",
                ],
            }
        ]
        assert pipeline.validation_errors["contacts"] == expected_response

        stage_id = db_session.query(Stage.id).filter_by(name="PROSPECTING").one()[0]
        product_id = db_session.query(Product.id).filter_by(name="BASIC").one()[0]
        forecast_id = (
            db_session.query(ForecastCategory.id).filter_by(name="PIPELINE").one()[0]
        )
        expected_response = [
            {
                "record": {
                    "id": "OP11111",
                    "name": "Company 1 - Basic Deal",
                    "contact_id": 1,
                    "company_id": 1,
                    "amount": 25000,
                    "probability": -20,
                    "created_date": "2024-06-15T08:45:00",
                    "close_date": "2024-09-15T08:45:00",
                    "is_closed": False,
                    "stage_id": stage_id,
                    "forecast_category_id": forecast_id,
                    "product_id": product_id,
                },
                "errors": ["probability: -20 must be between 0 and 100."],
            }
        ]
        assert pipeline.validation_errors["opportunities"] == expected_response
