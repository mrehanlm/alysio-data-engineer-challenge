import random
from datetime import datetime, timedelta

import pandas as pd


# Company data generation
def generate_companies(n=100):
    industries = ["Technology", "Healthcare", "Finance", "Manufacturing", "Retail"]
    sizes = ["1-10", "11-50", "51-200", "201-500", "501-1000", "1000+"]

    companies = []
    for i in range(n):
        company = {
            "id": f"COMP{i:03d}",
            "name": f"Company {i}",
            "domain": f"company{i}.com",
            "industry": random.choice(industries),
            "size": random.choice(sizes),
            "country": random.choice(["US", "UK", "CA", "AU", "DE", "FR"]),
            "created_date": (
                datetime.now() - timedelta(days=random.randint(0, 365))
            ).isoformat(),
            "is_customer": random.choice([True, False]),
            "annual_revenue": random.randint(100000, 10000000),
        }
        companies.append(company)
    return pd.DataFrame(companies)


# Contact data with duplicates and inconsistencies
def generate_contacts(companies, n=500):
    titles = ["CEO", "CTO", "VP Sales", "Director", "Manager", "Engineer"]
    domains = companies["domain"].tolist()

    contacts = []
    for i in range(n):
        domain = random.choice(domains)
        company = companies[companies["domain"] == domain].iloc[0]
        first_name = f"First{i}"
        last_name = f"Last{i}"

        # Introduce some duplicates with variations
        if i % 20 == 0:
            first_name = f"First{i-1}"  # Duplicate with variation

        contact = {
            "id": f"CONT{i:03d}",
            "email": f"{first_name.lower()}.{last_name.lower()}@{domain}",
            "first_name": first_name,
            "last_name": last_name,
            "title": random.choice(titles),
            "company_id": company["id"],
            "phone": f"+1-555-{random.randint(100,999)}-{random.randint(1000,9999)}",
            "status": random.choice(["Lead", "Qualified", "Customer", "Churned"]),
            "created_date": company["created_date"],
            "last_modified": (
                datetime.now() - timedelta(days=random.randint(0, 30))
            ).isoformat(),
        }
        contacts.append(contact)
    return pd.DataFrame(contacts)


# Opportunity data with complex relationships
def generate_opportunities(contacts, n=200):
    stages = [
        "Prospecting",
        "Qualification",
        "Proposal",
        "Negotiation",
        "Closed Won",
        "Closed Lost",
    ]
    products = ["Basic", "Pro", "Enterprise"]

    opportunities = []
    for i in range(n):
        contact = contacts.iloc[random.randint(0, len(contacts) - 1)]
        stage = random.choice(stages)
        product = random.choice(products)

        created_date = datetime.fromisoformat(contact["created_date"])
        close_date = created_date + timedelta(days=random.randint(30, 180))

        opportunity = {
            "id": f"OPP{i:03d}",
            "name": f'{contact["company_id"]} - {product} Deal',
            "contact_id": contact["id"],
            "company_id": contact["company_id"],
            "amount": random.randint(10000, 100000),
            "stage": stage,
            "product": product,
            "probability": random.randint(0, 100),
            "created_date": created_date.isoformat(),
            "close_date": close_date.isoformat(),
            "is_closed": stage in ["Closed Won", "Closed Lost"],
            "forecast_category": random.choice(
                ["Pipeline", "Best Case", "Commit", "Closed"]
            ),
        }
        opportunities.append(opportunity)
    return pd.DataFrame(opportunities)


# Activity data with varied types and relationships
def generate_activities(contacts, opportunities, n=1000):
    activity_types = ["email", "call", "meeting", "demo", "task"]

    activities = []
    for i in range(n):
        activity_type = random.choice(activity_types)
        contact = contacts.iloc[random.randint(0, len(contacts) - 1)]

        # Some activities linked to opportunities
        opp_id = None
        if random.random() < 0.3:
            opp = opportunities[opportunities["contact_id"] == contact["id"]]
            if not opp.empty:
                opp_id = opp.iloc[0]["id"]

        activity = {
            "id": f"ACT{i:03d}",
            "contact_id": contact["id"],
            "opportunity_id": opp_id,
            "type": activity_type,
            "subject": f'{activity_type.title()} with {contact["first_name"]}',
            "timestamp": (
                datetime.now() - timedelta(days=random.randint(0, 90))
            ).isoformat(),
            "duration_minutes": random.randint(15, 60),
            "outcome": random.choice(["Completed", "No Show", "Rescheduled"]),
            "notes": f"Sample notes for {activity_type}",
        }
        activities.append(activity)
    return pd.DataFrame(activities)


# Generate all data
companies = generate_companies(100)
contacts = generate_contacts(companies, 500)
opportunities = generate_opportunities(contacts, 200)
activities = generate_activities(contacts, opportunities, 1000)

# Save to multiple formats
companies.to_csv("data/salesforce/companies.csv", index=False)
contacts.to_json("data/salesforce/contacts.json", orient="records")
opportunities.to_csv("data/salesforce/opportunities.csv", index=False)
activities.to_json("data/salesforce/activities.json", orient="records")
