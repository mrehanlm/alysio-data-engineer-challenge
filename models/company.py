from datetime import datetime

import country_converter as cc
import validators
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import relationship

from utils.db import BaseModel
from utils.etl import standardize_datetime


class Industry(BaseModel):
    __tablename__ = "industries"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)

    # Relationships
    companies = relationship("Company", back_populates="industry")

    __table_args__ = (Index("idx_industries_name", "name"),)


class Company(BaseModel):
    __tablename__ = "companies"
    id = Column(String, primary_key=True)

    # FKs
    industry_id = Column(Integer, ForeignKey("industries.id"), nullable=False)

    name = Column(String, nullable=False)
    domain = Column(String, unique=True, nullable=False)
    size = Column(String, nullable=False)
    country = Column(String, nullable=False)
    created_date = Column(DateTime, nullable=False)
    is_customer = Column(Boolean, nullable=False)
    annual_revenue = Column(Float, nullable=False)

    # Relationships
    industry = relationship("Industry", back_populates="companies")
    contacts = relationship("Contact", back_populates="company")
    opportunities = relationship("Opportunity", back_populates="company")

    __table_args__ = (
        Index("idx_companies_domain", "domain"),
        Index("idx_companies_industry_id", "industry_id"),
    )

    def validate_name(self, key, name):
        return name.strip().upper()

    def validate_domain(self, key, domain):
        """
        Validates the format of a given domain string.

        Returns:
            str: The validated domain string if it is valid.

        Raises:
            ValueError: If the domain string is formatted incorrectly or logically invalid.
        """
        domain = domain.strip()
        is_valid = validators.domain(domain)
        if not is_valid:
            raise ValueError(f"{key}: {domain} is invalid.")
        return domain

    def validate_size(self, key, size):
        """
        Validates the format of a given size string.

        This method ensures that the provided size string follows an expected pattern.
        It checks for valid usage of "+" and "-" delimiters and enforces logical constraints.

        Returns:
            str: The validated size string if it passes all checks.

        Raises:
            ValueError: If the size string is formatted incorrectly or logically invalid.

        Valid formats:
            - "1000" (single value)
            - "1000+" (greater than or equal to 1000)
            - "1000-10000" (range with a valid lower and upper bound)

        Invalid formats:
            - "1000+100" (misplaced '+')
            - "1000-100-100000" (too many '-' separators)
            - "1000-100" (inverted range where the lower bound is greater than the upper bound)
        """
        size = size.strip()

        # example: 1000+100
        if "+" in size and not size.endswith("+"):
            raise ValueError(f"{key}: {size} is invalid.")

        parts = size.split("-")
        if len(parts) == 2:
            # example: 1000-100
            if int(parts[0]) > int(parts[1]):
                raise ValueError(f"{key}: {size} is invalid.")

        # example: 1000-10000-100000
        if len(parts) > 2:
            raise ValueError(f"{key}: {size} is invalid.")

        return size

    def validate_country(self, key, country):
        """
        Validates and standardizes a country name or code.

        This method ensures that the provided country value is properly formatted
        and converts it to its corresponding ISO 2-letter country code.

        Returns:
            str: The validated and standardized ISO2 country code.

        Raises:
            ValueError: If the provided country name or code is invalid.

        Examples:
            - Input: "United States"
              Output: "US"
            - Input: "USA"
              Output: "US"
            - Input: "France"
              Output: "FR"
            - Input: "XYZ" (invalid country)
              Raises ValueError
        """
        country = country.strip().upper()
        country = cc.convert(names=[country], to="ISO2", not_found=404)
        if country == 404:
            raise ValueError(f"{key}: {country} is invalid.")
        return country

    def validate_created_date(self, key, created_date):
        """
        Validates and standardizes a given created_date.

        This method ensures that the provided date is properly formatted,
        converts it to a standardized datetime format, and checks if it
        is not set in the future.

        Returns:
            datetime: The validated and standardized datetime object.

        Raises:
            ValueError: If the provided date is invalid or set in the future.

        Example:
            - Input: "2024-11-01T10:52:05.749559"
              Output: datetime(2024, 11, 1, 10, 52, 5, 749559)
            - Input: "2050-01-01" (future date)
              Raises ValueError
            - Input: "InvalidDate"
              Raises ValueError
        """
        try:
            created_date = standardize_datetime(created_date)
        except Exception:
            raise ValueError(
                f"{key}: {created_date} is invalid. Please consider ISO 8601 format."
            )

        if created_date > datetime.now(tz=created_date.tzinfo):
            raise ValueError(f"{key}: cannot be in future: {created_date}")

        return created_date
