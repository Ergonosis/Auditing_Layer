from pydantic import Field

PII_METADATA = {"pii": True, "classification": "restricted"}
SENSITIVE_METADATA = {"pii": False, "classification": "sensitive"}
PUBLIC_METADATA = {"pii": False, "classification": "public"}


def pii_field(**kwargs):
    return Field(**kwargs, json_schema_extra=PII_METADATA)


def sensitive_field(**kwargs):
    return Field(**kwargs, json_schema_extra=SENSITIVE_METADATA)


def public_field(**kwargs):
    return Field(**kwargs, json_schema_extra=PUBLIC_METADATA)
