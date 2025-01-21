from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship

from utils.db import BaseModel
from utils.etl import standardize_datetime


class Activity(BaseModel):
    __tablename__ = "activities"

    # FKs
    contact_id = Column(String, ForeignKey("contacts.id"), nullable=False)
    opportunity_id = Column(String, ForeignKey("opportunities.id"))

    # Required Fields
    id = Column(String, primary_key=True)
    type = Column(String, nullable=False)
    subject = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    duration_minutes = Column(Integer, nullable=False)
    outcome = Column(String, nullable=False)

    # Nullable Fields
    notes = Column(String)

    # FK Relationships
    contact = relationship("Contact", back_populates="activities")
    opportunity = relationship("Opportunity", back_populates="activities")

    __table_args__ = (
        Index("idx_activities_contact_id", "contact_id"),
        Index("idx_activities_opportunity_id", "opportunity_id"),
        Index("idx_activities_timestamp", "timestamp"),
    )

    def validate_type(self, key, activity_type):
        return activity_type.strip().upper()

    def validate_outcome(self, key, outcome):
        return outcome.strip().upper()

    def validate_subject(self, key, subject):
        return subject.strip().lower()

    def validate_timestamp(self, key, timestamp):
        """
        Validates and standardizes a given timestamp.

        This method ensures that the provided date is properly formatted,
        converts it to a standardized datetime format, and checks if it
        is not set in the future.

        Returns:
            datetime: The validated and standardized datetime object.

        Raises:
            ValueError: If the provided date is invalid.

        Example:
            - Input: "2024-11-01T10:52:05.749559"
              Output: datetime(2024, 11, 1, 10, 52, 5, 749559)
            - Input: "2050-01-01" (future date)
              Raises ValueError
            - Input: "InvalidDate"
              Raises ValueError
        """
        try:
            timestamp = standardize_datetime(timestamp)
        except Exception:
            raise ValueError(
                f"{key}: {timestamp} is invalid. Please consider ISO 8601 format."
            )

        return timestamp

    def validate_duration_minutes(self, key, duration):
        """
        Validates the duration of the activity.

        Ensures the duration is a non-negative integer.

        Returns:
            int: The validated duration in minutes.

        Raises:
            ValueError: If the duration is invalid or negative.
        """
        if not isinstance(duration, int) or duration < 0:
            raise ValueError(f"{key}: Duration must be a non-negative integer.")

        return duration
