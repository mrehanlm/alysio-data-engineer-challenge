from datetime import datetime

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


class Stage(BaseModel):
    __tablename__ = "stages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)

    opportunities = relationship("Opportunity", back_populates="stage")

    __table_args__ = (Index("idx_stages_name", "name"),)


class ForecastCategory(BaseModel):
    __tablename__ = "forecast_categories"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)

    opportunities = relationship("Opportunity", back_populates="forecast_category")

    __table_args__ = (Index("idx_forecast_categories_name", "name"),)


class Product(BaseModel):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)

    opportunities = relationship("Opportunity", back_populates="product")

    __table_args__ = (Index("idx_products_name", "name"),)


class Opportunity(BaseModel):
    __tablename__ = "opportunities"
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)

    # FKs
    contact_id = Column(String, ForeignKey("contacts.id"), nullable=False)
    company_id = Column(String, ForeignKey("companies.id"), nullable=False)
    stage_id = Column(Integer, ForeignKey("stages.id"), nullable=False)
    forecast_category_id = Column(
        Integer, ForeignKey("forecast_categories.id"), nullable=False
    )
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)

    # fields
    amount = Column(Float, nullable=False)
    probability = Column(Integer, nullable=False)
    created_date = Column(DateTime, nullable=False)
    close_date = Column(DateTime, nullable=False)
    is_closed = Column(Boolean, nullable=False)

    # Relationships
    contact = relationship("Contact", back_populates="opportunities")
    company = relationship("Company", back_populates="opportunities")
    stage = relationship("Stage", back_populates="opportunities")
    forecast_category = relationship("ForecastCategory", back_populates="opportunities")
    product = relationship("Product", back_populates="opportunities")
    activities = relationship("Activity", back_populates="opportunity")

    __table_args__ = (
        Index("idx_opportunities_contact_id", "contact_id"),
        Index("idx_opportunities_company_id", "company_id"),
        Index("idx_opportunities_stage_id", "stage_id"),
        Index("idx_opportunities_product_id", "product_id"),
        Index("idx_opportunities_forecast_category_id", "forecast_category_id"),
    )

    def validate_name(self, key, name):
        return name.strip().title()

    def validate_probability(self, key, probability):
        """
        Validates the probability.

        Ensures the probability is between 0 and 100.
        """
        try:
            probability = int(probability)
        except ValueError:
            raise ValueError(f"{key}: {probability} must be a valid integer.")

        if not (0 <= probability <= 100):
            raise ValueError(f"{key}: {probability} must be between 0 and 100.")

        return probability

    def validate_amount(self, key, amount):
        """
        Validates the amount value.

        Ensures the amount is a non-negative float.

        Returns:
            float: The validated amount.

        Raises:
            ValueError: If the amount is negative or not a valid float.
        """
        try:
            amount = float(amount)
        except ValueError:
            raise ValueError(f"{key}: {amount} must be a valid numeric value.")

        if amount < 0:
            raise ValueError(f"{key}: {amount} cannot be negative.")

        return amount

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
            raise ValueError(
                f"{key}: Created date cannot be in the future: {created_date}"
            )
        return created_date

    def validate_close_date(self, key, close_date):
        """
        Validates and standardizes the close_date.

        Ensures the date is valid and not in the future.

        Returns:
            datetime: The validated and standardized close_date.

        Raises:
            ValueError: If the date is invalid or set in the future.
        """
        try:
            close_date = standardize_datetime(close_date)
        except Exception:
            raise ValueError(
                f"{key}: {close_date} is invalid. Please use ISO 8601 format."
            )

        return close_date
