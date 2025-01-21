"""
Sample queries to retrieve data inserted by ETL pipeline from the database
"""

import logging

import sqlparse
from sqlalchemy import and_
from sqlalchemy.orm import joinedload

from models import Activity, Company, Contact, ContactStatus, Opportunity
from utils.db import db_session

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_user_activities(session, user_id):
    """
    Get a user's activities based on the contact_id, limited to 10 records.
    """
    activities = (
        session.query(Activity).filter(Activity.contact_id == user_id).limit(10)
    )

    print("-------------------------------------------------")
    print(" Activities for User {user_id} (LIMIT 10)")
    print("-------------------------------------------------")
    print("\n --------- SQL Start ---------")
    print(
        f"\n {sqlparse.format(f"{activities}", reindent=True, keyword_case='upper')} \n"
    )
    print("--------- SQL End --------- \n")

    print("\nOUTPUT: \n")
    for activity in activities.all():
        logger.info(
            "Activity ID: %s - Subject: %s - Timestamp: %s",
            activity.id,
            activity.subject,
            activity.timestamp,
        )


def get_companies_with_high_revenue(session):
    """
    Get companies that are customers and have an annual revenue greater than 1M, limited to 10 records.
    """
    companies = (
        session.query(Company)
        .filter(Company.is_customer.is_(True), Company.annual_revenue > 1_000_000)
        .limit(10)
    )

    print("-------------------------------------------------")
    print(" Companies that are Customers with Revenue > 1M (LIMIT 10)")
    print("-------------------------------------------------")
    print("\n --------- SQL Start ---------")
    print(
        f"\n {sqlparse.format(f"{companies}", reindent=True, keyword_case='upper')} \n"
    )
    print("--------- SQL End --------- \n")

    print("\nOUTPUT: \n")
    for company in companies.all():
        logger.info("Company: %s - Revenue: %s", company.name, company.annual_revenue)


def get_contacts_by_status_and_joined_load(session):
    """
    Get contacts by status and perform a joined load for company, opportunities, and activities, limited to 10 records.
    """
    status = "LEAD"
    contacts = (
        session.query(Contact)
        .join(
            ContactStatus,
            and_(ContactStatus == status, ContactStatus.id == Contact.status_id),
        )
        .options(
            joinedload(Contact.company),
            joinedload(Contact.opportunities),
            joinedload(Contact.activities),
        )
        .limit(10)
    )

    print("-------------------------------------------------")
    print(" Contacts with Status: {status} (LIMIT 10)")
    print("-------------------------------------------------")
    print("\n --------- SQL Start ---------")
    print(
        f"\n {sqlparse.format(f"{contacts}", reindent=True, keyword_case='upper')} \n"
    )
    print("--------- SQL End --------- \n")

    print("\nOUTPUT: \n")
    for contact in contacts.all():
        logger.info("Contact: %s %s", contact.first_name, contact.last_name)
        logger.info(
            "Company: %s", contact.company.name if contact.company else "No Company"
        )
        logger.info(
            "Opportunities: %s",
            [opportunity.name for opportunity in contact.opportunities],
        )
        logger.info(
            "Activities: %s", [activity.subject for activity in contact.activities]
        )


def get_activities_with_opportunities(session):
    """
    Get activities and join load opportunities, limited to 10 records.
    """
    activities_with_opportunities = (
        session.query(Activity).options(joinedload(Activity.opportunity)).limit(10)
    )

    print("-------------------------------------------------")
    print(" Activities with associated Opportunities (LIMIT 10)")
    print("-------------------------------------------------")
    print("\n --------- SQL Start ---------")
    print(
        f"\n {sqlparse.format(f"{activities_with_opportunities}", reindent=True, keyword_case='upper')} \n"
    )
    print("--------- SQL End --------- \n")

    print("\nOUTPUT: \n")
    for activity in activities_with_opportunities.all():
        logger.info("Activity: %s", activity.subject)
        logger.info(
            "Opportunity: %s",
            activity.opportunity.name if activity.opportunity else "No Opportunity",
        )


def get_opportunities_with_activities(session):
    """
    Get opportunities and join load activities, limited to 10 records.
    """
    opportunities_with_activities = (
        session.query(Opportunity).options(joinedload(Opportunity.activities)).limit(10)
    )

    print("-------------------------------------------------")
    print(" Opportunities with associated Activities (LIMIT 10)")
    print("-------------------------------------------------")
    print("\n --------- SQL Start ---------")
    print(
        f"\n {sqlparse.format(f"{opportunities_with_activities}", reindent=True, keyword_case='upper')} \n"
    )
    print("--------- SQL End --------- \n")

    print("\nOUTPUT: \n")
    for opportunity in opportunities_with_activities.all():
        logger.info("Opportunity: %s", opportunity.name)
        logger.info(
            "Activities: %s", [activity.subject for activity in opportunity.activities]
        )


if __name__ == "__main__":

    user_id = "CONT095"
    with db_session() as session:
        get_user_activities(session, user_id)
        get_companies_with_high_revenue(session)
        get_contacts_by_status_and_joined_load(session)
        get_activities_with_opportunities(session)
        get_opportunities_with_activities(session)
