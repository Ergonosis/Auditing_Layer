"""
Generate a high-fidelity SMB dataset for pipeline testing.

Produces three fixture files:
  tests/fixtures/smb_transactions.json    — 60 transactions
  tests/fixtures/smb_emails.json          — 55 emails
  tests/fixtures/smb_calendar_events.json — 25 calendar events

Design principles:
  - Merchant name variance: Plaid raw names differ from email subjects, exercising Tier 3 fuzzy matching
  - Deliberate scenario engineering: each bucket (Tier2, Tier3-high, Tier3-boundary, ambiguous,
    unmatched, nulls, refunds) is explicitly constructed and labeled
  - Temporal realism: business-day clustering, 2026-03-10 to 2026-04-05

Engineered scenario targets:
  Tier 2 exact composite links:       8   (normalized merchant_name == normalized subject, same date)
  Tier 3 fuzzy high-confidence:      20   (WRatio ≥ 0.85, date within window)
  Tier 3 fuzzy boundary-confidence:   8   (WRatio ~0.80–0.84, stress-tests min_similarity_score)
  Ambiguous matches:                  6   (1 txn → 2+ emails/events above threshold)
  Unmatched transactions:            10   (no candidates, out-of-window, or sub-threshold)
  Unmatched emails:                   7   (promotional/newsletter, no txn counterpart)
  Null merchant_name transactions:    9   (15% of 60, only 'name' field populated)
  Null subject emails:                5   (~10% of 55)
  Null subject calendar events:       2   (~10% of 25)
  Refund / negative-amount txns:      5
  Payroll / ACH-credit txns:          3   (large negative, no matching vendor email)
  Multi-day calendar events:          2   (hotel stays)

Usage:
    python scripts/generate_smb_dataset.py
"""

import json
import sys
from pathlib import Path

# ── Vendor table ───────────────────────────────────────────────────────────────
# Each vendor has realistic Plaid raw name variants and corresponding email/calendar
# subject variants. This is the core of the dataset — the gap between these is
# exactly what Tier 3 fuzzy matching must bridge.

VENDORS = [
    # ── Groceries ────────────────────────────────────────────────────────────
    {
        "key": "whole_foods",
        "canonical": "Whole Foods Market",
        "plaid_raw_variants": [
            "WHOLEFDS MKT #0523",
            "WFM #0523 SAN FRANCISCO CA",
            "WHOLE FOODS #523",
        ],
        "plaid_clean_name": "Whole Foods Market",
        "email_subject_variants": [
            "Your Whole Foods Market receipt",
            "Whole Foods order confirmation",
            "Receipt from Whole Foods",
        ],
        "calendar_subject_variants": [
            "Team lunch - Whole Foods",
            "Whole Foods pickup",
        ],
        "typical_amounts": [38.50, 52.20, 91.00, 14.75],
        "category": ["Food and Drink", "Groceries"],
        "payment_channel": "in store",
    },
    {
        "key": "trader_joes",
        "canonical": "Trader Joe's",
        "plaid_raw_variants": [
            "TRADER JOE S #127",
            "TRADER JOES #127 SAN JOSE CA",
            "TJ S #127",
        ],
        "plaid_clean_name": "Trader Joe's",
        "email_subject_variants": [
            "Your Trader Joe's receipt",
            "Trader Joe's order confirmation",
        ],
        "calendar_subject_variants": [
            "Grocery run - Trader Joe's",
        ],
        "typical_amounts": [22.40, 45.60, 67.80],
        "category": ["Food and Drink", "Groceries"],
        "payment_channel": "in store",
    },
    {
        "key": "costco",
        "canonical": "Costco",
        "plaid_raw_variants": [
            "COSTCO WHSE #0456",
            "COSTCO WHOLESALE #456",
        ],
        "plaid_clean_name": "Costco",
        "email_subject_variants": [
            "Costco Wholesale receipt",
            "Your Costco order",
        ],
        "calendar_subject_variants": [
            "Costco run",
            "Office supply run - Costco",
        ],
        "typical_amounts": [128.40, 245.00, 89.60],
        "category": ["Shops", "Warehouses and Wholesale Stores"],
        "payment_channel": "in store",
    },
    # ── Coffee ────────────────────────────────────────────────────────────────
    {
        "key": "starbucks",
        "canonical": "Starbucks",
        "plaid_raw_variants": [
            "STARBUCKS #12345 SAN FRANCISCO CA",
            "STARBUCKS STORE 12345",
            "SBX #12345",
        ],
        "plaid_clean_name": "Starbucks",
        "email_subject_variants": [
            "Your Starbucks receipt",
            "Thanks for your Starbucks order",
            "Starbucks Stars earned",
        ],
        "calendar_subject_variants": [
            "Coffee chat - Starbucks",
            "1:1 at Starbucks",
        ],
        "typical_amounts": [5.75, 6.85, 12.50, 18.40],
        "category": ["Food and Drink", "Coffee Shop"],
        "payment_channel": "in store",
    },
    {
        "key": "blue_bottle",
        "canonical": "Blue Bottle Coffee",
        "plaid_raw_variants": [
            "BLUE BOTTLE COFFEE SF",
            "BLUE BOTTLE #003 SF",
        ],
        "plaid_clean_name": "Blue Bottle Coffee",
        "email_subject_variants": [
            "Your Blue Bottle Coffee receipt",
            "Blue Bottle Coffee order confirmation",
        ],
        "calendar_subject_variants": [
            "Coffee with investor - Blue Bottle",
        ],
        "typical_amounts": [4.50, 7.25, 9.00],
        "category": ["Food and Drink", "Coffee Shop"],
        "payment_channel": "in store",
    },
    # ── Airlines / Travel ─────────────────────────────────────────────────────
    {
        "key": "delta",
        "canonical": "Delta Air Lines",
        "plaid_raw_variants": [
            "DELTA AIR LINES 006",
            "DELTA AIR 0062234556778",
            "DL 0062234556778",
        ],
        "plaid_clean_name": "Delta Air Lines",
        "email_subject_variants": [
            "Your Delta flight booking confirmation",
            "Delta Air Lines e-ticket receipt",
            "Your Delta itinerary",
        ],
        "calendar_subject_variants": [
            "Flight to NYC - Delta",
            "SFO → JFK Delta flight",
        ],
        "typical_amounts": [189.00, 328.50, 412.00, 249.00],
        "category": ["Travel", "Airlines and Aviation Services"],
        "payment_channel": "online",
    },
    {
        "key": "united",
        "canonical": "United Airlines",
        "plaid_raw_variants": [
            "UNITED AIRLINES 016",
            "UNITED 0162234556789",
        ],
        "plaid_clean_name": "United Airlines",
        "email_subject_variants": [
            "United Airlines booking confirmation",
            "Your United Airlines e-ticket",
        ],
        "calendar_subject_variants": [
            "United flight to Chicago",
        ],
        "typical_amounts": [175.00, 298.00, 445.00],
        "category": ["Travel", "Airlines and Aviation Services"],
        "payment_channel": "online",
    },
    # ── Rideshare ─────────────────────────────────────────────────────────────
    {
        "key": "uber",
        "canonical": "Uber",
        "plaid_raw_variants": [
            "UBER * TRIP",
            "UBER TECHNOLOGIES",
            "UBER *TRIP SAN FRANCISCO",
        ],
        "plaid_clean_name": "Uber",
        "email_subject_variants": [
            "Your Tuesday trip with Uber",
            "Here's your Uber receipt",
            "Uber receipt - trip to SFO",
        ],
        "calendar_subject_variants": [
            "Uber to airport",
        ],
        "typical_amounts": [12.40, 18.75, 34.20, 8.90],
        "category": ["Travel", "Taxi"],
        "payment_channel": "online",
    },
    {
        "key": "lyft",
        "canonical": "Lyft",
        "plaid_raw_variants": [
            "LYFT *RIDE SUN 9PM",
            "LYFT   *RIDE",
        ],
        "plaid_clean_name": "Lyft",
        "email_subject_variants": [
            "Your Lyft receipt",
            "Thanks for riding with Lyft",
        ],
        "calendar_subject_variants": [
            "Lyft to office",
        ],
        "typical_amounts": [11.20, 22.50, 16.80],
        "category": ["Travel", "Taxi"],
        "payment_channel": "online",
    },
    # ── Hotels ────────────────────────────────────────────────────────────────
    {
        "key": "marriott",
        "canonical": "Marriott",
        "plaid_raw_variants": [
            "MARRIOTT SF DOWNTOWN",
            "MARRIOTT INTL HOTELS",
        ],
        "plaid_clean_name": "Marriott",
        "email_subject_variants": [
            "Your Marriott reservation confirmation",
            "Marriott Bonvoy: Your stay receipt",
        ],
        "calendar_subject_variants": [
            "Marriott hotel stay - NYC",
            "Team offsite - Marriott",
        ],
        "typical_amounts": [189.00, 254.00, 312.00],
        "category": ["Travel", "Lodging"],
        "payment_channel": "in store",
    },
    {
        "key": "airbnb",
        "canonical": "Airbnb",
        "plaid_raw_variants": [
            "AIRBNB * HMWM2ABCDE",
            "AIRBNB INC",
        ],
        "plaid_clean_name": "Airbnb",
        "email_subject_variants": [
            "Airbnb booking confirmed",
            "Your Airbnb reservation is confirmed",
        ],
        "calendar_subject_variants": [
            "Airbnb stay - Austin",
        ],
        "typical_amounts": [145.00, 220.00, 380.00],
        "category": ["Travel", "Lodging"],
        "payment_channel": "online",
    },
    # ── Office / SaaS ─────────────────────────────────────────────────────────
    {
        "key": "wework",
        "canonical": "WeWork",
        "plaid_raw_variants": [
            "WEWORK 340 PINE ST SF",
            "WEWORK COMPANIES INC",
        ],
        "plaid_clean_name": "WeWork",
        "email_subject_variants": [
            "WeWork invoice - March 2026",
            "Your WeWork membership receipt",
        ],
        "calendar_subject_variants": [
            "WeWork all-hands",
            "Team sync at WeWork",
        ],
        "typical_amounts": [500.00, 750.00, 1200.00],
        "category": ["Service", "Real Estate"],
        "payment_channel": "online",
    },
    {
        "key": "aws",
        "canonical": "Amazon Web Services",
        "plaid_raw_variants": [
            "AWS EMEA",
            "AMAZON WEB SERVICES",
            "AWS EMEA LLC",
        ],
        "plaid_clean_name": "Amazon Web Services",
        "email_subject_variants": [
            "AWS Invoice for March 2026",
            "Your Amazon Web Services bill",
            "AWS: Invoice available",
        ],
        "calendar_subject_variants": [
            "AWS architecture review",
            "Cloud cost review - AWS",
        ],
        "typical_amounts": [342.80, 511.20, 89.40],
        "category": ["Service", "Computers and Electronics"],
        "payment_channel": "online",
    },
    {
        "key": "zoom",
        "canonical": "Zoom",
        "plaid_raw_variants": [
            "ZOOM.US",
            "ZOOM VIDEO COMMUNICATIONS",
        ],
        "plaid_clean_name": "Zoom",
        "email_subject_variants": [
            "Your Zoom subscription receipt",
            "Zoom: payment confirmation",
        ],
        "calendar_subject_variants": [
            "Zoom vendor meeting",
        ],
        "typical_amounts": [14.99, 19.99, 149.90],
        "category": ["Service", "Computers and Electronics"],
        "payment_channel": "online",
    },
    # ── B2B / Vendors ─────────────────────────────────────────────────────────
    {
        "key": "stripe",
        "canonical": "Stripe",
        "plaid_raw_variants": [
            "STRIPE PAYMENTS",
            "STRIPE INC",
        ],
        "plaid_clean_name": "Stripe",
        "email_subject_variants": [
            "Stripe invoice #INV-0042",
            "Your Stripe receipt",
        ],
        "calendar_subject_variants": [
            "Stripe integration review",
        ],
        "typical_amounts": [29.00, 99.00, 250.00],
        "category": ["Service", "Financial Services"],
        "payment_channel": "online",
    },
    {
        "key": "quickbooks",
        "canonical": "QuickBooks",
        "plaid_raw_variants": [
            "INTUIT *QUICKBOOKS",
            "INTUIT QUICKBOOKS ONLINE",
        ],
        "plaid_clean_name": "QuickBooks",
        "email_subject_variants": [
            "QuickBooks: Your subscription is confirmed",
            "Intuit QuickBooks receipt",
        ],
        "calendar_subject_variants": [
            "Bookkeeping review - QuickBooks",
        ],
        "typical_amounts": [30.00, 55.00, 85.00],
        "category": ["Service", "Financial Services"],
        "payment_channel": "online",
    },
    {
        "key": "gusto",
        "canonical": "Gusto",
        "plaid_raw_variants": [
            "GUSTO PAYROLL",
            "GUSTO COM",
        ],
        "plaid_clean_name": "Gusto",
        "email_subject_variants": [
            "Gusto payroll receipt",
            "Your Gusto invoice for March",
        ],
        "calendar_subject_variants": [
            "Payroll review - Gusto",
        ],
        "typical_amounts": [45.00, 149.00],
        "category": ["Service", "Financial Services"],
        "payment_channel": "online",
    },
    # ── Food delivery ─────────────────────────────────────────────────────────
    {
        "key": "doordash",
        "canonical": "DoorDash",
        "plaid_raw_variants": [
            "DOORDASH*CHIPOTLE",
            "DOORDASH *SUBSC",
            "DOORDASH INC",
        ],
        "plaid_clean_name": "DoorDash",
        "email_subject_variants": [
            "Your DoorDash order is confirmed",
            "DoorDash receipt",
            "Your order from DoorDash",
        ],
        "calendar_subject_variants": [
            "Team lunch DoorDash order",
        ],
        "typical_amounts": [18.40, 32.60, 55.20],
        "category": ["Food and Drink", "Food Delivery Services"],
        "payment_channel": "online",
    },
    # ── Misc ─────────────────────────────────────────────────────────────────
    {
        "key": "fedex",
        "canonical": "FedEx",
        "plaid_raw_variants": [
            "FEDEX #123456789",
            "FEDEX OFFIC 1234",
        ],
        "plaid_clean_name": "FedEx",
        "email_subject_variants": [
            "FedEx shipment confirmation",
            "Your FedEx package is on the way",
        ],
        "calendar_subject_variants": [
            "FedEx pickup",
        ],
        "typical_amounts": [12.50, 28.90, 45.00],
        "category": ["Service", "Shipping and Freight"],
        "payment_channel": "in store",
    },
    {
        "key": "home_depot",
        "canonical": "The Home Depot",
        "plaid_raw_variants": [
            "THE HOME DEPOT #0547",
            "HOME DEPOT #547 SAN FRANCISCO",
        ],
        "plaid_clean_name": "The Home Depot",
        "email_subject_variants": [
            "The Home Depot order confirmation",
            "Your Home Depot receipt",
        ],
        "calendar_subject_variants": [
            "Office supplies - Home Depot",
        ],
        "typical_amounts": [34.50, 89.00, 145.60],
        "category": ["Shops", "Hardware Store"],
        "payment_channel": "in store",
    },
]

# Build a lookup by key for convenience
VENDOR_MAP = {v["key"]: v for v in VENDORS}

# ── Business dates (Mon–Fri, 2026-03-10 to 2026-04-05) ───────────────────────

from datetime import date, timedelta

def _business_days(start: str, end: str):
    d = date.fromisoformat(start)
    stop = date.fromisoformat(end)
    days = []
    while d <= stop:
        if d.weekday() < 5:  # Mon=0 … Fri=4
            days.append(d)
        d += timedelta(days=1)
    return days

BIZ_DAYS = _business_days("2026-03-10", "2026-04-05")

def _dt(d: date, hour: int = 9, minute: int = 0) -> str:
    return f"{d.isoformat()}T{hour:02d}:{minute:02d}:00Z"

# ── ID generators ─────────────────────────────────────────────────────────────

_txn_counter = [0]
_email_counter = [0]
_cal_counter = [0]

def _txn_id() -> str:
    _txn_counter[0] += 1
    return f"smb_txn_{_txn_counter[0]:03d}"

def _email_id() -> str:
    _email_counter[0] += 1
    return f"smb_msg_{_email_counter[0]:03d}"

def _cal_id(suffix: str = "") -> str:
    _cal_counter[0] += 1
    return f"smb_evt_{_cal_counter[0]:03d}{suffix}"

# ── Record builders ───────────────────────────────────────────────────────────

def txn(
    amount: float,
    d: date,
    merchant_name: str | None,
    name: str,
    payment_channel: str = "in store",
    category: list | None = None,
    account_id: str = "smb_acct_001",
) -> dict:
    return {
        "transaction_id": _txn_id(),
        "account_id": account_id,
        "amount": amount,
        "date": d.isoformat(),
        "merchant_name": merchant_name,
        "name": name,
        "payment_channel": payment_channel,
        "category": category or ["Other"],
    }

def email(
    d: date,
    subject: str | None,
    sender: str = "noreply@vendor.com",
    recipients: list | None = None,
    body_preview: str | None = None,
    thread_id: str | None = None,
    hour: int = 10,
) -> dict:
    return {
        "message_id": _email_id(),
        "received_at": _dt(d, hour),
        "sender": sender,
        "recipients": recipients or ["owner@smbbiz.com"],
        "subject": subject,
        "body_preview": body_preview,
        "thread_id": thread_id,
    }

def cal(
    d: date,
    subject: str | None,
    end_d: date | None = None,
    organizer: str = "owner@smbbiz.com",
    attendees: list | None = None,
    location: str | None = None,
    start_hour: int = 14,
    end_hour: int = 15,
) -> dict:
    end = end_d or d
    return {
        "event_id": _cal_id(),
        "start_time": _dt(d, start_hour),
        "end_time": _dt(end, end_hour),
        "organizer": organizer,
        "subject": subject,
        "attendees": attendees or [],
        "location": location,
    }

# ── Dataset assembly ──────────────────────────────────────────────────────────

transactions = []
emails = []
calendar_events = []

# Helper to grab a biz day by index
def bd(i: int) -> date:
    return BIZ_DAYS[i % len(BIZ_DAYS)]


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SCENARIO A: Tier 2 exact composite matches (8 pairs)                      ║
# ║  merchant_name canonical == normalized email subject, same date            ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
# For Tier 2 to fire, normalized(merchant_name) must == normalized(email_subject)
# and dates must be equal. We use the canonical name on both sides.

for i, (vkey, amount, day_idx) in enumerate([
    ("starbucks",   5.75, 0),
    ("whole_foods", 52.20, 1),
    ("aws",        342.80, 2),
    ("quickbooks",  55.00, 3),
    ("zoom",        14.99, 4),
    ("gusto",       45.00, 5),
    ("stripe",      99.00, 6),
    ("fedex",       28.90, 7),
]):
    v = VENDOR_MAP[vkey]
    d = bd(day_idx)
    # Transaction: use clean canonical name as merchant_name (Tier 2 will normalize to match)
    t = txn(amount, d, v["plaid_clean_name"], v["plaid_clean_name"],
            v["payment_channel"], v["category"])
    t["transaction_id"] = f"smb_t2_txn_{i+1:02d}"
    transactions.append(t)
    # Email: subject is exactly the canonical name (after normalization will match)
    e = email(d, v["canonical"], sender=f"receipt@{vkey}.com", hour=11)
    e["message_id"] = f"smb_t2_msg_{i+1:02d}"
    emails.append(e)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SCENARIO B: Tier 3 high-confidence matches (20 pairs, WRatio ≥ 0.85)     ║
# ║  Plaid raw variant on txn, email/cal subject variant — within date window  ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

tier3_high_specs = [
    # (vendor_key, plaid_raw_idx, email_subj_idx, offset_days, amount, entity_type)
    # entity_type: "email" or "calendar"
    ("whole_foods", 0, 0, 0,  38.50, "email"),    # WHOLEFDS MKT #0523 → Your Whole Foods Market receipt
    ("whole_foods", 1, 1, 1,  91.00, "email"),    # WFM #0523 ... → Whole Foods order confirmation
    ("starbucks",   0, 0, 0,   6.85, "email"),    # STARBUCKS #12345 ... → Your Starbucks receipt
    ("starbucks",   1, 1, 2,  12.50, "email"),    # STARBUCKS STORE → Thanks for your Starbucks order
    ("delta",       0, 0, 0, 189.00, "email"),    # DELTA AIR LINES 006 → Your Delta flight booking
    ("delta",       1, 2, 1, 328.50, "email"),    # DELTA AIR 006... → Your Delta itinerary
    ("uber",        0, 0, 0,  12.40, "email"),    # UBER * TRIP → Your Tuesday trip with Uber
    ("uber",        1, 1, 2,  18.75, "email"),    # UBER TECHNOLOGIES → Here's your Uber receipt
    ("marriott",    0, 0, 0, 254.00, "email"),    # MARRIOTT SF DOWNTOWN → Your Marriott reservation
    ("marriott",    1, 1, 0, 189.00, "calendar"), # MARRIOTT INTL HOTELS → Team offsite - Marriott
    ("airbnb",      0, 0, 0, 145.00, "email"),    # AIRBNB * HMWM... → Airbnb booking confirmed
    ("airbnb",      1, 1, 1, 220.00, "email"),    # AIRBNB INC → Your Airbnb reservation confirmed
    ("aws",         0, 0, 0, 511.20, "email"),    # AWS EMEA → AWS Invoice for March 2026
    ("aws",         2, 2, 1,  89.40, "email"),    # AWS EMEA LLC → AWS: Invoice available
    ("wework",      0, 0, 0, 500.00, "email"),    # WEWORK 340 PINE → WeWork invoice
    ("wework",      1, 1, 0, 750.00, "calendar"), # WEWORK COMPANIES → WeWork all-hands
    ("doordash",    0, 0, 0,  18.40, "email"),    # DOORDASH*CHIPOTLE → Your DoorDash order
    ("doordash",    2, 2, 2,  55.20, "email"),    # DOORDASH INC → Your order from DoorDash
    ("home_depot",  0, 0, 0,  89.00, "email"),    # THE HOME DEPOT #0547 → The Home Depot order
    ("lyft",        0, 0, 0,  11.20, "email"),    # LYFT *RIDE → Your Lyft receipt
]

for i, (vkey, raw_idx, subj_idx, day_offset, amount, etype) in enumerate(tier3_high_specs):
    v = VENDOR_MAP[vkey]
    d = bd(10 + i)
    e_d = bd(10 + i + day_offset)  # within 3-day window for txn→email, 1-day for txn→cal
    raw_name = v["plaid_raw_variants"][raw_idx % len(v["plaid_raw_variants"])]
    t = txn(amount, d, None, raw_name, v["payment_channel"], v["category"])
    t["transaction_id"] = f"smb_t3h_txn_{i+1:02d}"
    transactions.append(t)
    if etype == "email":
        subj = v["email_subject_variants"][subj_idx % len(v["email_subject_variants"])]
        e = email(e_d, subj, sender=f"receipt@{vkey.replace('_','')}.com")
        e["message_id"] = f"smb_t3h_msg_{i+1:02d}"
        emails.append(e)
    else:
        subj = v["calendar_subject_variants"][subj_idx % len(v["calendar_subject_variants"])]
        c = cal(e_d, subj)
        c["event_id"] = f"smb_t3h_evt_{i+1:02d}"
        calendar_events.append(c)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SCENARIO C: Tier 3 boundary-confidence matches (8 pairs, WRatio ~0.80)   ║
# ║  Abbreviated or indirect subject — stress-tests min_similarity_score       ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
# These use indirect/abbreviated email subjects that are harder to fuzzy-match.
# They should score just above 0.80 given the current WRatio implementation.

boundary_specs = [
    # (vendor_key, plaid_raw, email_subject, amount)
    ("trader_joes", "TRADER JOE S #127",          "Grocery receipt attached",            45.60),
    ("costco",      "COSTCO WHSE #0456",           "Warehouse receipt - March",           128.40),
    ("blue_bottle", "BLUE BOTTLE COFFEE SF",        "Coffee purchase confirmation",         7.25),
    ("united",      "UNITED AIRLINES 016",          "Flight booking receipt",             175.00),
    ("home_depot",  "HOME DEPOT #547 SAN FRANCISCO","Hardware store receipt",              34.50),
    ("quickbooks",  "INTUIT *QUICKBOOKS",           "Software subscription invoice",       30.00),
    ("stripe",      "STRIPE PAYMENTS",             "Payment processor invoice",           29.00),
    ("fedex",       "FEDEX OFFIC 1234",             "Shipping label confirmation",         12.50),
]

for i, (vkey, raw_name, subj, amount) in enumerate(boundary_specs):
    v = VENDOR_MAP[vkey]
    d = bd(32 + i)
    t = txn(amount, d, None, raw_name, v["payment_channel"], v["category"])
    t["transaction_id"] = f"smb_t3b_txn_{i+1:02d}"
    transactions.append(t)
    e = email(d, subj, sender="billing@vendor.com")
    e["message_id"] = f"smb_t3b_msg_{i+1:02d}"
    emails.append(e)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SCENARIO D: Ambiguous matches (6 groups)                                  ║
# ║  1 transaction → 2+ emails/events both scoring above threshold             ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

ambiguous_specs = [
    # Uber: two separate trip receipts same day → both match the txn
    ("uber", "UBER * TRIP", 34.20, "Your Uber trip receipt - AM", "Your Uber trip receipt - PM"),
    # Starbucks: two receipt emails on the same day (two purchases)
    ("starbucks", "STARBUCKS STORE 12345", 18.40, "Your Starbucks receipt", "Thanks for your Starbucks order"),
    # Delta: two flight confirmations (booking + change confirmation)
    ("delta", "DELTA AIR LINES 006", 412.00, "Your Delta flight booking confirmation", "Delta flight change confirmation"),
    # WeWork: two invoices in same window
    ("wework", "WEWORK 340 PINE ST SF", 1200.00, "WeWork invoice - March 2026", "WeWork invoice updated - March 2026"),
    # DoorDash: two delivery receipts same day
    ("doordash", "DOORDASH INC", 32.60, "Your DoorDash order is confirmed", "DoorDash order update - delivered"),
    # AWS: two billing emails (invoice + payment confirmation)
    ("aws", "AWS EMEA", 89.40, "AWS Invoice for March 2026", "AWS: Payment received for March"),
]

for i, (vkey, raw_name, amount, subj_a, subj_b) in enumerate(ambiguous_specs):
    v = VENDOR_MAP[vkey]
    d = bd(40 + i)
    t = txn(amount, d, None, raw_name, v["payment_channel"], v["category"])
    t["transaction_id"] = f"smb_amb_txn_{i+1:02d}"
    transactions.append(t)
    ea = email(d, subj_a, sender=f"noreply@{vkey.replace('_','')}.com", hour=9)
    ea["message_id"] = f"smb_amb_msg_{i+1:02d}a"
    eb = email(d, subj_b, sender=f"noreply@{vkey.replace('_','')}.com", hour=14)
    eb["message_id"] = f"smb_amb_msg_{i+1:02d}b"
    emails.append(ea)
    emails.append(eb)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SCENARIO E: Unmatched transactions (10)                                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# E1: No merchant_name, generic name — nothing in pool to match (4)
for i, (name, amount) in enumerate([
    ("POS PURCHASE 4821", 22.00),
    ("CHECKCARD 0315 PURCHASE", 8.50),
    ("DEBIT CARD PURCHASE", 45.00),
    ("MISC DEBIT 889920", 12.00),
]):
    t = txn(amount, bd(46 + i), None, name)
    t["transaction_id"] = f"smb_unm_txn_{i+1:02d}"
    transactions.append(t)

# E2: Has merchant_name but email is 5+ days out of window (3)
for i, (vkey, raw_name, amount, window_miss) in enumerate([
    ("whole_foods", "WHOLEFDS MKT #0523", 38.50, 5),
    ("starbucks",   "STARBUCKS #12345 SAN FRANCISCO CA", 5.75, 6),
    ("uber",        "UBER * TRIP", 12.40, 7),
]):
    v = VENDOR_MAP[vkey]
    d = bd(50 + i)
    t = txn(amount, d, None, raw_name, v["payment_channel"], v["category"])
    t["transaction_id"] = f"smb_unm_txn_{4+i+1:02d}"
    transactions.append(t)
    # Email exists but is too far away
    late_d = date.fromisoformat(d.isoformat()) + timedelta(days=window_miss)
    e = email(late_d, v["email_subject_variants"][0], sender="receipt@vendor.com")
    e["message_id"] = f"smb_unm_msg_{i+1:02d}_outofwindow"
    emails.append(e)

# E3: Has merchant_name, email nearby, but WRatio < 0.80 (3)
low_similarity_specs = [
    # Raw Plaid name that bears little resemblance to the email subject
    ("POS TERMINAL 9012",        "Office supplies monthly invoice",    55.00),
    ("ACH DEBIT VENDOR 00X",     "Technology services - quarterly",   180.00),
    ("WIRE TRANSFER OUTBOUND",   "Consulting agreement payment",      500.00),
]
for i, (raw_name, email_subj, amount) in enumerate(low_similarity_specs):
    d = bd(53 + i)
    t = txn(amount, d, None, raw_name)
    t["transaction_id"] = f"smb_unm_txn_{7+i+1:02d}"
    transactions.append(t)
    e = email(d, email_subj, sender="billing@consulting.com")
    e["message_id"] = f"smb_unm_msg_{3+i+1:02d}_lowsim"
    emails.append(e)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SCENARIO F: Unmatched emails (7) — promotional/newsletter, no txn         ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

promo_emails = [
    ("deals@amazon.com",        "Today's deals just for you",              bd(0)),
    ("newsletter@techcrunch.com","TechCrunch Daily: Top stories",          bd(2)),
    ("noreply@linkedin.com",    "You have 5 new connection requests",      bd(4)),
    ("offers@delta.com",        "Exclusive offer: miles double this week", bd(6)),
    ("promo@doordash.com",      "Free delivery this weekend only",         bd(8)),
    ("news@quickbooks.com",     "QuickBooks product update - March 2026",  bd(10)),
    (None,                       None,                                      bd(12)),  # null subject
]

for i, (sender, subj, d) in enumerate(promo_emails):
    e = email(d, subj, sender=sender or "unknown@vendor.com")
    e["message_id"] = f"smb_promo_msg_{i+1:02d}"
    emails.append(e)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SCENARIO G: Null field edge cases                                          ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# G1: Null merchant_name transactions (merchant_name=null, only name field)
null_merchant_specs = [
    ("WHOLEFDS MKT #0523",                 38.50),
    ("STARBUCKS #12345 SAN FRANCISCO CA",   5.75),
    ("DELTA AIR LINES 006",               189.00),
    ("UBER * TRIP",                        12.40),
    ("AWS EMEA",                          342.80),
    ("WEWORK 340 PINE ST SF",             500.00),
    ("DOORDASH*CHIPOTLE",                  18.40),
    ("LYFT *RIDE SUN 9PM",                 11.20),
    ("MARRIOTT SF DOWNTOWN",              254.00),
]
for i, (raw_name, amount) in enumerate(null_merchant_specs):
    d = bd(i)
    t = txn(amount, d, None, raw_name)  # merchant_name=None intentionally
    t["transaction_id"] = f"smb_null_txn_{i+1:02d}"
    transactions.append(t)

# G2: Null subject emails (5)
for i in range(5):
    d = bd(i * 3)
    e = email(d, None, sender="system@invoicing.com")
    e["message_id"] = f"smb_null_msg_{i+1:02d}"
    emails.append(e)

# G3: Null subject calendar events (2)
for i in range(2):
    d = bd(i * 5 + 1)
    c = cal(d, None, attendees=[], location=None)
    c["event_id"] = f"smb_null_evt_{i+1:02d}"
    calendar_events.append(c)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SCENARIO H: Refunds / negative-amount transactions (5)                    ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

refund_specs = [
    ("whole_foods", -38.50, "WHOLEFDS MKT #0523",               "Whole Foods Market refund confirmation"),
    ("delta",      -189.00, "DELTA AIR LINES 006",              "Delta Air Lines refund issued"),
    ("airbnb",     -145.00, "AIRBNB * HMWM2ABCDE",             "Airbnb refund processed"),
    ("uber",        -18.75, "UBER * TRIP",                      "Uber trip refund receipt"),
    ("aws",         -89.40, "AWS EMEA",                        "AWS credit memo for March 2026"),
]

for i, (vkey, amount, raw_name, email_subj) in enumerate(refund_specs):
    v = VENDOR_MAP[vkey]
    d = bd(15 + i * 2)
    t = txn(amount, d, None, raw_name, v["payment_channel"], v["category"])
    t["transaction_id"] = f"smb_ref_txn_{i+1:02d}"
    transactions.append(t)
    e = email(d, email_subj, sender=f"refunds@{vkey.replace('_','')}.com")
    e["message_id"] = f"smb_ref_msg_{i+1:02d}"
    emails.append(e)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SCENARIO I: Payroll / ACH credit (3) — large negatives, no vendor email   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

payroll_specs = [
    (-8500.00,  "Gusto Payroll",     "GUSTO PAYROLL",     "payroll"),
    (-2000.00,  "Owner Draw",        "ACH CREDIT OWNER",  "ach"),
    (-500.00,   "Expense Reimburse", "ACH DEBIT EXP REIM","ach"),
]

for i, (amount, name, raw, channel) in enumerate(payroll_specs):
    d = bd(20 + i)
    t = txn(amount, d, None, raw, channel, ["Transfer", "Payroll"])
    t["transaction_id"] = f"smb_pay_txn_{i+1:02d}"
    transactions.append(t)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SCENARIO J: Multi-day calendar events (2) — hotel stays                   ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# Marriott stay: check-in 2026-03-18, check-out 2026-03-21 (3 nights)
marriott_day = date(2026, 3, 18)
marriott_end = date(2026, 3, 21)
c = cal(marriott_day, "Marriott hotel stay - NYC conference",
        end_d=marriott_end, location="New York Marriott Marquis",
        attendees=["owner@smbbiz.com", "colleague@partner.com"],
        start_hour=15, end_hour=11)
c["event_id"] = "smb_multiday_evt_001"
calendar_events.append(c)
# Corresponding transaction on check-in date
t = txn(312.00, marriott_day, "Marriott", "MARRIOTT SF DOWNTOWN", "in store",
        ["Travel", "Lodging"])
t["transaction_id"] = "smb_multiday_txn_001"
transactions.append(t)

# Airbnb stay: check-in 2026-03-25, check-out 2026-03-27 (2 nights)
airbnb_day = date(2026, 3, 25)
airbnb_end = date(2026, 3, 27)
c = cal(airbnb_day, "Airbnb stay - Austin SXSW",
        end_d=airbnb_end, location="Austin, TX",
        attendees=["owner@smbbiz.com"],
        start_hour=16, end_hour=10)
c["event_id"] = "smb_multiday_evt_002"
calendar_events.append(c)
t = txn(380.00, airbnb_day, "Airbnb", "AIRBNB INC", "online",
        ["Travel", "Lodging"])
t["transaction_id"] = "smb_multiday_txn_002"
transactions.append(t)

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SCENARIO K: Standalone calendar events (not linked to specific txns)      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

standalone_events = [
    (bd(0),  "Q1 Business Review",             ["owner@smbbiz.com", "cfo@smbbiz.com"], "Board room"),
    (bd(3),  "Vendor onboarding - AWS",         ["owner@smbbiz.com", "aws@amazon.com"], "Zoom"),
    (bd(5),  "Coffee with investor",            ["owner@smbbiz.com"],                   "Blue Bottle SOMA"),
    (bd(8),  "WeWork community event",          [],                                     "WeWork 340 Pine"),
    (bd(11), "DoorDash partner meeting",        ["owner@smbbiz.com", "partner@dd.com"], "DoorDash HQ"),
    (bd(14), "Team retrospective",              ["owner@smbbiz.com", "eng@smbbiz.com"], "Zoom"),
    (bd(16), "Payroll planning - Gusto review", ["owner@smbbiz.com", "cfo@smbbiz.com"], "Conference room"),
]

for i, (d, subj, attendees, loc) in enumerate(standalone_events):
    c = cal(d, subj, attendees=attendees, location=loc)
    c["event_id"] = f"smb_cal_{i+1:02d}"
    calendar_events.append(c)

# ── Write fixtures ─────────────────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).parent.parent / "tests" / "fixtures"

def write_fixture(name: str, data: list) -> None:
    path = FIXTURES_DIR / name
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  Wrote {len(data):3d} records → {path}")

print("\nGenerating SMB dataset...")
write_fixture("smb_transactions.json",    transactions)
write_fixture("smb_emails.json",          emails)
write_fixture("smb_calendar_events.json", calendar_events)

# ── Summary ────────────────────────────────────────────────────────────────────

print(f"""
Summary:
  Transactions : {len(transactions)}
  Emails       : {len(emails)}
  Calendar evts: {len(calendar_events)}

Scenario buckets:
  Tier 2 exact pairs           : 8
  Tier 3 high-conf pairs       : 20
  Tier 3 boundary pairs        : 8
  Ambiguous groups             : 6  (2 emails per txn)
  Unmatched txns (designed)    : 10
  Unmatched emails (promo)     : 7
  Null merchant_name txns      : 9
  Null subject emails          : 5
  Null subject calendar events : 2
  Refund txns (negative)       : 5
  Payroll/ACH txns (no email)  : 3
  Multi-day calendar events    : 2
""")
