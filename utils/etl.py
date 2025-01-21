from datetime import timezone

import pandas as pd
import phonenumbers
from dateutil import parser

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def clean_text(text, lower=False):
    """
    Clean and standardize text.
    """
    if text is not None:
        text = text.strip()
        return text.lower() if lower else text.upper()


def standardize_datetime(datetime_str):
    """
    Parses any datetime format and add timezone info.
    """

    if isinstance(datetime_str, pd.Timestamp):
        # If the input is a Timestamp, ensure it is timezone-aware
        if datetime_str.tz is None:
            return datetime_str.tz_localize("UTC")
        return datetime_str

    parsed_dt = parser.parse(datetime_str)
    # Ensure timezone info is included; assume UTC if none is present
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)

    return parsed_dt


def normalize_phone_number(phone_number, region):
    """
    Normalize phone number to international format.

    Arguments:
        number: The number that we are attempting to parse. This can
                contain formatting such as +, ( and -, as well as a phone
                number extension. It can also be provided in RFC3966 format.
        region: The region that we are expecting the number to be from. This
                is only used if the number being parsed is not written in
                international format. The country_code for the number in
                this case would be stored as that of the default region
                supplied. If the number is guaranteed to start with a '+'
                followed by the country calling code, then None can be supplied.

    Returns:
        An international formatted phone number if normalized successfully and a boolean
        indicating whether the phone number was successfully parsed or not.
    """
    try:
        parsed_phone_number = phonenumbers.parse(phone_number, region)
        phone_number_format = phonenumbers.PhoneNumberFormat.INTERNATIONAL
        formatted_phone_number = phonenumbers.format_number(
            parsed_phone_number, phone_number_format
        )
        return formatted_phone_number, True
    except Exception:
        return phone_number, False
