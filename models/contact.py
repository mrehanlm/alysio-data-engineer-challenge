from datetime import datetime

import validators
from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, exists
from sqlalchemy.orm import relationship

from utils.db import BaseModel, db_session
from utils.etl import normalize_phone_number, standardize_datetime


class ContactStatus(BaseModel):
    __tablename__ = "contact_statuses"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)

    # Relationships
    contacts = relationship("Contact", back_populates="status")

    __table_args__ = (Index("idx_contact_statuses_name", "name"),)


class Contact(BaseModel):
    __tablename__ = "contacts"
    id = Column(String, primary_key=True)

    # FKs
    status_id = Column(Integer, ForeignKey("contact_statuses.id"), nullable=False)
    company_id = Column(String, ForeignKey("companies.id"), nullable=False)

    email = Column(String, unique=True, nullable=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    phone = Column(String, nullable=True)

    created_date = Column(DateTime, nullable=False)
    last_modified = Column(DateTime, nullable=False)

    # Relationships
    company = relationship("Company", back_populates="contacts")
    status = relationship("ContactStatus", back_populates="contacts")
    opportunities = relationship("Opportunity", back_populates="contact")
    activities = relationship("Activity", back_populates="contact")

    __table_args__ = (
        Index("idx_contacts_email", "email"),
        Index("idx_contacts_company_id", "company_id"),
        Index("idx_contacts_status_id", "status_id"),
    )

    def validate_email(self, key, email):
        """
        Validates the email address format.
        """
        email = email.strip()
        is_valid = validators.email(email)
        if not is_valid:
            raise ValueError(f"{key}: {email} is not a valid email address.")

        with db_session() as session:
            contact_found = session.query(
                exists().where(Contact.email == email)
            ).scalar()
            if contact_found:
                raise ValueError(f"{key}: A contact already exists with email {email}.")

        return email

    def validate_first_name(self, key, name):
        return name.strip().title()

    def validate_last_name(self, key, name):
        return name.strip().title()

    def validate_title(self, key, title):
        return title.strip()

    def validate_phone(self, key, phone):
        """
        Validates the phone number format and normalize it
        to standard international format.

        Returns:
            str: The normalized phone number.

        Raises:
            ValueError: If the phone number is invalid.
        """
        phone, is_normalized = normalize_phone_number(phone.strip(), None)
        if not is_normalized:
            raise ValueError(
                f"{key}: {phone} is not a valid. "
                "Please provide a valid phone number in international format."
            )
        return phone

    def validate_created_date(self, key, created_date):
        """
        Validates and standardizes the created_date.

        Ensures the date is valid and not in the future.

        Returns:
            datetime: The validated and standardized created_date.

        Raises:
            ValueError: If the date is invalid or set in the future.
        """
        try:
            created_date = standardize_datetime(created_date)
        except Exception:
            raise ValueError(
                f"{key}: {created_date} is invalid. Please use ISO 8601 format."
            )

        if created_date > datetime.now(tz=created_date.tzinfo):
            raise ValueError(f"{key}: cannot be in the future {created_date}")
        return created_date

    def validate_last_modified(self, key, last_modified):
        """
        Validates and standardizes the last_modified date.

        Ensures the date is valid, not in the future, and not before created_date.

        Returns:
            datetime: The validated and standardized last_modified date.

        Raises:
            ValueError: If the date is invalid or logically incorrect.
        """
        try:
            last_modified = standardize_datetime(last_modified)
        except Exception:
            raise ValueError(
                f"{key}: {last_modified} is invalid. Please use ISO 8601 format."
            )

        if last_modified > datetime.now(tz=last_modified.tzinfo):
            raise ValueError(f"{key}: cannot be in the future {last_modified}")

        if hasattr(self, "created_date") and last_modified < self.created_date:
            raise ValueError(f"{key}: cannot be before `created_date`.")
        return last_modified
