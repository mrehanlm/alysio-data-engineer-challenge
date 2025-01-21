import json
import logging
import os
import time
from typing import Dict, Union

import pandas as pd
from sqlalchemy.orm import Session

from models import (
    Activity,
    Company,
    Contact,
    ContactStatus,
    ForecastCategory,
    Industry,
    Opportunity,
    Product,
    Stage,
)
from utils.db import db_session
from utils.etl import clean_text

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Pipeline:

    # chunk size for reading large CSV files
    chunk_size = 50000

    validation_errors = {
        "companies": [],
        "contacts": [],
        "opportunities": [],
        "activities": [],
    }

    def __init__(self, path, errors_path):
        self.data_path = path
        self.errors_path = errors_path

    def run(self):
        with db_session() as session:
            self.process_companies(session)
            self.process_contacts(session)
            self.process_opportunities(session)
            self.process_activities(session)

        # log errors to a json file
        if any(len(errors) > 0 for errors in self.validation_errors.values()):
            self.log_errors()
            logger.info(
                "\n ETL Pipeline has been successfully completed."
                f"\n There were validation errors. Please check them out at: {self.errors_path}"
            )
        else:
            logger.info("\n ETL Pipeline has been successfully completed.")

    def log_errors(self):
        """
        Writes the validation errors to a JSON file.
        """
        errors_file_path = os.path.join(
            self.errors_path, f"{time.time()}-pipeline-errors.json"
        )
        if self.validation_errors:
            # Write the updated errors back to the file
            os.makedirs(self.errors_path, exist_ok=True)
            with open(errors_file_path, "w") as f:
                json.dump(self.validation_errors, f, indent=4)

    def process_sub_entities(
        self,
        session: Session,
        sub_entity_class: Union[
            Industry, Product, ContactStatus, ForecastCategory, Stage
        ],
        entities: pd.DataFrame,
        entity_column: str,
    ) -> Dict[str, int]:
        """
        Processes and inserts unique `sub_entity_class`'s model instances into the database.

        This method:
        1. Extracts unique `entity_column` values from the provided entities dataset.
        2. Retrieves existing `sub_entity_class` instances from the database given above extracted values.
        3. Inserts new `sub_entity_class` instances that do not already exist.
        4. Returns a mapping: `sub_entity_class.name` to `sub_entity_class.id`.

        Args:
            session (Session): SQLAlchemy database session.
            sub_entity_class (class): SQLAlchemy model class.
            entities (DataFrame): Pandas DataFrame containing entities
            with `entity_column` column.
            entity_column (str): The column name in the entities DataFrame.

        Returns:
            dict: A mapping of `sub_entity_class`'s names to their corresponding database IDs.
        """
        # Clean up to get unique `entity_column` values
        unique_sub_entity_values = entities[entity_column].apply(clean_text).unique()

        # Retrieve existing `sub_entity_class` instances from database
        name_col = getattr(sub_entity_class, "name")
        id_col = getattr(sub_entity_class, "id")
        existing_sub_entities = session.query(id_col, name_col).filter(
            name_col.in_(unique_sub_entity_values)
        )

        # Bulk insert new `sub_entity_class` instances
        new_sub_entities = set(unique_sub_entity_values) - set(
            [industry.name for industry in existing_sub_entities.all()]
        )
        session.bulk_save_objects(
            [sub_entity_class(name=name) for name in new_sub_entities]
        )
        session.commit()

        # Return `sub_entity_class` instances map to be used
        # during the ETL processing
        return {
            sub_entity.name: sub_entity.id for sub_entity in existing_sub_entities.all()
        }

    def process_companies(self, session: Session):
        """
        Processes and inserts company data into the database.

        This method:
        1. Reads company data from a CSV file.
        2. Processes and maps industries to their corresponding IDs.
        3. Identifies new companies that are not already in the database.
        4. Cleans, validates, and prepares new companies for insertion.
        5. Performs a bulk insert of new companies.

        Args:
            session (Session): SQLAlchemy database session.
        """
        companies_df_chunk_iter = pd.read_csv(
            os.path.join(self.data_path, "companies.csv"), chunksize=self.chunk_size
        )
        for companies_df in companies_df_chunk_iter:
            # Process industries data first
            industry_ids_map = self.process_sub_entities(
                session, Industry, companies_df, "industry"
            )

            # Convert company data into a dictionary with company ID as key
            companies_map = {
                company["id"]: company
                for company in companies_df.to_dict(orient="records")
            }

            # Identify existing companies
            company_ids = list(companies_map.keys())
            existing_companies = session.query(Company.id).filter(
                Company.id.in_(company_ids)
            )

            # Determine which companies are new
            new_company_ids = set(company_ids) - set(
                [company.id for company in existing_companies.all()]
            )

            # Clean and validate new companies
            to_create = []
            for company_id in new_company_ids:
                # Replace industry name with industry ID (foreign key)
                company_data = companies_map[company_id]
                company_data["industry_id"] = industry_ids_map[
                    clean_text(company_data.pop("industry"))
                ]
                company = Company(**company_data)

                # Validate company data before insertion
                is_valid = company.validate()
                if not is_valid:
                    self.validation_errors["companies"].append(
                        {"record": company_data, "errors": company.validation_errors}
                    )
                    continue

                to_create.append(company)

            # Bulk insert new companies
            session.bulk_save_objects(to_create)
            session.commit()

    def process_contacts(self, session: Session):
        """
        Processes and inserts contact data into the database.

        This method:
        1. Reads contact data from a CSV file.
        2. Identifies new contacts that are not already in the database.
        3. Cleans, validates, and prepares new contacts for insertion.
        4. Performs a bulk insert of new contacts.

        Args:
            session (Session): SQLAlchemy database session.
        """
        # Read contacts data from a JSON file

        # NOTE: `chunksize` cannot be used because JSONReader requires file to
        # be in NDJSON format (i.e. newline-delimited JSON). This can be addressed
        # in data generation script.

        contacts = pd.read_json(os.path.join(self.data_path, "contacts.json"))
        # Sorting contacts by last_modified date and removing duplicates
        # by keeping first record since it will be newest
        contacts.sort_values(by="last_modified", ascending=False, inplace=True)
        contacts.drop_duplicates(subset="email", inplace=True, keep="first")

        contact_statuses_map = self.process_sub_entities(
            session, ContactStatus, contacts, "status"
        )

        # Convert contact data into a dictionary with contact ID as key
        contacts_map = {
            contact["id"]: contact for contact in contacts.to_dict(orient="records")
        }

        # Identify existing contacts
        contact_ids = list(contacts_map.keys())
        existing_contacts = session.query(Contact.id).filter(
            Contact.id.in_(contact_ids)
        )

        # Determine which contacts are new
        new_contact_ids = set(contact_ids) - set(
            [contact.id for contact in existing_contacts.all()]
        )

        # Clean and validate new contacts
        to_create = []
        for contact_id in new_contact_ids:
            contact_data = contacts_map[contact_id]
            contact_data["status_id"] = contact_statuses_map[
                clean_text(contact_data.pop("status"))
            ]

            contact = Contact(**contact_data)

            # Validate contact data before insertion
            is_valid = contact.validate()
            if not is_valid:
                self.validation_errors["contacts"].append(
                    {"record": contact_data, "errors": contact.validation_errors}
                )
                continue

            to_create.append(contact)

        # Bulk insert new contacts
        session.bulk_save_objects(to_create)
        session.commit()

    def process_opportunities(self, session: Session):
        """
        Processes and inserts opportunity data into the database.

        This method:
        1. Reads opportunity data from a CSV file.
        2. Identifies new opportunities that are not already in the database.
        3. Cleans, validates, and prepares new opportunities for insertion.
        4. Performs a bulk insert of new opportunities.

        Args:
            session (Session): SQLAlchemy database session.
        """
        opportunities_df_chunk_iter = pd.read_csv(
            os.path.join(self.data_path, "opportunities.csv"), chunksize=self.chunk_size
        )
        for opportunities_df in opportunities_df_chunk_iter:
            # Process stages, forecast categories, and products to get their IDs
            stages_map = self.process_sub_entities(
                session, Stage, opportunities_df, "stage"
            )
            forecast_categories_map = self.process_sub_entities(
                session, ForecastCategory, opportunities_df, "forecast_category"
            )
            products_map = self.process_sub_entities(
                session, Product, opportunities_df, "product"
            )

            # Convert opportunity data into a dictionary with opportunity ID as key
            opportunities_map = {
                opportunity["id"]: opportunity
                for opportunity in opportunities_df.to_dict(orient="records")
            }

            # Identify existing opportunities
            opportunity_ids = list(opportunities_map.keys())
            existing_opportunities = session.query(Opportunity.id).filter(
                Opportunity.id.in_(opportunity_ids)
            )

            # Determine which opportunities are new
            new_opportunity_ids = set(opportunity_ids) - set(
                [opportunity.id for opportunity in existing_opportunities.all()]
            )

            # Clean and validate new opportunities
            to_create = []
            for opportunity_id in new_opportunity_ids:
                opportunity_data = opportunities_map[opportunity_id]

                # Map stage, forecast category, and product IDs
                opportunity_data["stage_id"] = stages_map[
                    clean_text(opportunity_data.pop("stage"))
                ]
                opportunity_data["forecast_category_id"] = forecast_categories_map[
                    clean_text(opportunity_data.pop("forecast_category"))
                ]
                opportunity_data["product_id"] = products_map[
                    clean_text(opportunity_data.pop("product"))
                ]

                # Create the Opportunity instance
                opportunity = Opportunity(**opportunity_data)

                # Validate the opportunity
                is_valid = opportunity.validate()
                if not is_valid:
                    self.validation_errors["opportunities"].append(
                        {
                            "record": opportunity_data,
                            "errors": opportunity.validation_errors,
                        }
                    )
                    continue

                to_create.append(opportunity)

            # Bulk insert new opportunities
            session.bulk_save_objects(to_create)
            session.commit()

    def process_activities(self, session: Session) -> None:
        """
        Processes and inserts activity data into the database.

        This method:
        1. Reads activity data from a CSV file.
        2. Identifies new activities that are not already in the database.
        3. Cleans, validates, and prepares new activities for insertion.
        4. Performs a bulk insert of new activities.

        Args:
            session (Session): SQLAlchemy database session.
        """
        # Read activities data from a JSON file

        # NOTE: `chunksize` cannot be used because JSONReader requires file to
        # be in NDJSON format (i.e. newline-delimited JSON). This can be addressed
        # in data generation script.

        activities = pd.read_json(os.path.join(self.data_path, "activities.json"))

        # Convert activity data into a dictionary with activity ID as key
        activities_map = {
            activity["id"]: activity
            for activity in activities.to_dict(orient="records")
        }

        # Identify existing activities
        activity_ids = list(activities_map.keys())
        existing_activities = session.query(Activity.id).filter(
            Activity.id.in_(activity_ids)
        )

        # Determine which activities are new
        new_activity_ids = set(activity_ids) - set(
            [activity.id for activity in existing_activities.all()]
        )

        # Clean and validate new activities
        to_create = []
        for activity_id in new_activity_ids:
            activity_data = activities_map[activity_id]

            activity = Activity(**activity_data)

            # Validate activity data before insertion
            is_valid = activity.validate()
            if not is_valid:
                self.validation_errors["activities"].append(
                    {"record": activity_data, "errors": activity.validation_errors}
                )
                continue

            to_create.append(activity)

        # Bulk insert new activities
        session.bulk_save_objects(to_create)
        session.commit()


if __name__ == "__main__":
    pipeline = Pipeline(path="data/salesforce/", errors_path="data/errors/")
    pipeline.run()
