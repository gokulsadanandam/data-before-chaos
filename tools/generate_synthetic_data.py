"""Generate messy, multi-source synthetic customer data with ground truth.

Every record is derived from a known synthetic *person*, so matching quality can be
measured exactly. The generator plants the problems the pipeline must handle:

* the same person in 1-5 systems with nicknames, typos, casing, swapped name order
* phone numbers in many formats; email domain typos (gmial.com); missing values
* households sharing a phone or email (different first names, same surname/address)
* shared and placeholder values (9999999999, test@test.com, "Walk In Customer")
* different people with the same common name in different cities (hard negatives)
* conflicting consent between systems; re-delivered rows; orders and campaign sends

Output layout (matches config/sources.yaml)::

    <out>/erp_a/customers_001.csv, orders_001.csv
    <out>/erp_b/customers_001.csv, orders_001.csv
    <out>/crm/contacts_001.csv
    <out>/web/signups_001.json
    <out>/pos_loyalty/members_001.csv
    <out>/mobile/profiles_001.json
    <out>/engagement/sends_001.csv
    <out>/_ground_truth/truth.csv            record_id, person_id
    <out>/_ground_truth/labelled_pairs.csv   left_id, right_id, is_match, pair_type

All values are fictitious. No real personal data is used.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from itertools import combinations
from pathlib import Path
from typing import Any

FIRST_NAMES = [
    "Aarav",
    "Vivaan",
    "Aditya",
    "Arjun",
    "Rohan",
    "Karan",
    "Rahul",
    "Vikram",
    "Sanjay",
    "Amit",
    "Priya",
    "Ananya",
    "Diya",
    "Kavya",
    "Neha",
    "Pooja",
    "Sneha",
    "Meera",
    "Isha",
    "Riya",
    "William",
    "Robert",
    "Elizabeth",
    "Katherine",
    "Michael",
    "Jennifer",
    "Thomas",
    "Margaret",
    "Suresh",
    "Ramesh",
    "Lakshmi",
    "Deepa",
    "Ganesh",
    "Harish",
    "Nisha",
    "Farah",
    "Imran",
    "Zoya",
]
NICKNAMES = {
    "William": ["Bill", "Will"],
    "Robert": ["Bob", "Rob"],
    "Elizabeth": ["Liz", "Beth"],
    "Katherine": ["Kate", "Kathy"],
    "Michael": ["Mike"],
    "Jennifer": ["Jen"],
    "Thomas": ["Tom"],
    "Margaret": ["Maggie", "Peggy"],
    "Vikram": ["Vicky"],
    "Ramesh": ["Ramu"],
    "Suresh": ["Suri"],
}
LAST_NAMES = [
    "Sharma",
    "Verma",
    "Gupta",
    "Iyer",
    "Nair",
    "Reddy",
    "Patel",
    "Shah",
    "Mehta",
    "Rao",
    "Kumar",
    "Singh",
    "Das",
    "Bose",
    "Menon",
    "Pillai",
    "Joshi",
    "Kulkarni",
    "Desai",
    "Khan",
    "Fernandes",
    "D'Souza",
    "Chatterjee",
    "Banerjee",
    "Mukherjee",
    "Agarwal",
    "Malhotra",
    "Kapoor",
]
COMMON_NAMES = [("Priya", "Sharma"), ("Rahul", "Kumar"), ("Amit", "Singh"), ("Neha", "Gupta")]
CITIES = [
    ("Bengaluru", "5600"),
    ("Mumbai", "4000"),
    ("Delhi", "1100"),
    ("Chennai", "6000"),
    ("Hyderabad", "5000"),
    ("Pune", "4110"),
    ("Kolkata", "7000"),
    ("Ahmedabad", "3800"),
    ("Kochi", "6820"),
    ("Jaipur", "3020"),
]
STREETS = [
    "MG Road",
    "Brigade Road",
    "Linking Road",
    "Park Street",
    "Anna Salai",
    "FC Road",
    "Ring Road",
    "Station Road",
    "Church Street",
    "Lake View Road",
]
DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "rediffmail.com"]
DOMAIN_TYPOS = {
    "gmail.com": ["gmial.com", "gmai.com", "gamil.com"],
    "yahoo.com": ["yaho.com"],
    "hotmail.com": ["hotmial.com"],
    "outlook.com": ["outlok.com"],
}
COUNTRY_VARIANTS = {"erp_a": ["IN"], "erp_b": ["India", "INDIA", "IND"], "crm": ["India", "IN", "Bharat"]}
CAMPAIGNS = ["DIWALI_26", "MONSOON_SALE", "NEW_YEAR_27", "LOYALTY_BOOST"]
CHANNEL_COST = {"email": 0.02, "sms": 0.15, "whatsapp": 0.35}
SOURCES = ("erp_a", "erp_b", "crm", "web", "pos_loyalty", "mobile")
SOURCE_PRESENCE = {
    "erp_a": 0.35,
    "erp_b": 0.30,
    "crm": 0.55,
    "web": 0.45,
    "pos_loyalty": 0.40,
    "mobile": 0.35,
}


@dataclass
class Person:
    person_id: str
    first: str
    last: str
    email_local: str
    domain: str
    phone10: str
    street_no: int
    street: str
    city: str
    postcode: str
    tax_id: str | None
    loyalty: str | None
    consent: dict[str, str]
    household: str | None = None
    records: list[str] = field(default_factory=list)

    @property
    def email(self) -> str:
        return f"{self.email_local}@{self.domain}"


# ----------------------------------------------------------------------------- noise helpers
def typo(rng: random.Random, word: str) -> str:
    if len(word) < 4:
        return word
    i = rng.randrange(1, len(word) - 1)
    op = rng.choice(["swap", "drop", "double"])
    if op == "swap":
        return word[:i] + word[i + 1] + word[i] + word[i + 2 :]
    if op == "drop":
        return word[:i] + word[i + 1 :]
    return word[:i] + word[i] + word[i:]


def vary_first(rng: random.Random, first: str) -> str:
    r = rng.random()
    if r < 0.15 and first in NICKNAMES:
        return rng.choice(NICKNAMES[first])
    if r < 0.22:
        return typo(rng, first)
    if r < 0.27:
        return first[0] + "."
    return first


def vary_case(rng: random.Random, text: str) -> str:
    r = rng.random()
    if r < 0.15:
        return text.upper()
    if r < 0.25:
        return text.lower()
    if r < 0.30:
        return f"  {text} "
    return text


def phone_format(rng: random.Random, p10: str) -> str:
    return rng.choice(
        [
            p10,
            f"+91{p10}",
            f"+91 {p10[:5]} {p10[5:]}",
            f"0{p10}",
            f"91-{p10}",
            f"({p10[:3]}) {p10[3:6]}-{p10[6:]}",
            f"0091{p10}",
            f"{p10[:5]}-{p10[5:]}",
        ]
    )


def email_variant(rng: random.Random, p: Person) -> str:
    r = rng.random()
    if r < 0.08 and p.domain in DOMAIN_TYPOS:
        return f"{p.email_local}@{rng.choice(DOMAIN_TYPOS[p.domain])}"
    if r < 0.15:
        return p.email.upper()
    if r < 0.18:
        return f" {p.email} "
    return p.email


def ts(rng: random.Random, fmt: str) -> str:
    moment = datetime(2024, 1, 1) + timedelta(days=rng.randrange(0, 900), seconds=rng.randrange(0, 86400))
    return moment.strftime(fmt)


def consent_text(rng: random.Random, value: str, style: str) -> str:
    yes, no = {"yn": ("Y", "N"), "bool": ("true", "false"), "num": ("1", "0"), "word": ("Yes", "No")}[style]
    if rng.random() < 0.05:
        return ""
    return yes if value == "yes" else no


# ----------------------------------------------------------------------------- people
def make_people(rng: random.Random, n: int) -> list[Person]:
    people: list[Person] = []
    used_phones: set[str] = set()
    for i in range(n):
        if rng.random() < 0.04:
            first, last = rng.choice(COMMON_NAMES)  # hard negatives: same name, different person
        else:
            first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        city, pin_prefix = rng.choice(CITIES)
        phone = ""
        while not phone or phone in used_phones:
            phone = str(rng.choice([6, 7, 8, 9])) + "".join(str(rng.randrange(10)) for _ in range(9))
        used_phones.add(phone)
        local_base = f"{first}.{last}".lower().replace("'", "")
        local = rng.choice(
            [local_base, f"{local_base}{rng.randrange(1, 99)}", f"{first[0]}{last}".lower().replace("'", "")]
        )
        people.append(
            Person(
                person_id=f"P{i:06d}",
                first=first,
                last=last,
                email_local=f"{local}{i % 97}" if rng.random() < 0.5 else local + str(i),
                domain=rng.choice(DOMAINS),
                phone10=phone,
                street_no=rng.randrange(1, 400),
                street=rng.choice(STREETS),
                city=city,
                postcode=f"{pin_prefix}{rng.randrange(10, 99)}",
                tax_id=(
                    f"{''.join(rng.choice('ABCDEFGHJKLMNPQRSTUVWXYZ') for _ in range(5))}"
                    f"{rng.randrange(1000, 9999)}{rng.choice('ABCDEFGHJKLMNPQRSTUVWXYZ')}"
                )
                if rng.random() < 0.3
                else None,
                loyalty=f"LC{rng.randrange(10_000_000, 99_999_999)}" if rng.random() < 0.5 else None,
                consent={c: ("yes" if rng.random() < 0.6 else "no") for c in ("email", "sms", "whatsapp")},
            )
        )
    # Households: a second person sharing phone (and often email), surname and address.
    for i in range(0, n, 25):
        if i + 1 >= n:
            break
        a, b = people[i], people[i + 1]
        b.last, b.phone10, b.street_no, b.street, b.city, b.postcode = (
            a.last,
            a.phone10,
            a.street_no,
            a.street,
            a.city,
            a.postcode,
        )
        if b.first == a.first:
            b.first = rng.choice([f for f in FIRST_NAMES if f != a.first])
        if rng.random() < 0.4:
            b.email_local, b.domain = a.email_local, a.domain
        b.tax_id = None
        a.household = b.household = f"H{i:06d}"
    return people


# ----------------------------------------------------------------------------- source rows
def erp_a_row(rng: random.Random, p: Person, rid: str) -> dict[str, Any]:
    name = f"{vary_first(rng, p.first)} {p.last}" if rng.random() < 0.8 else f"{p.last}, {p.first}"
    return {
        "KUNNR": rid,
        "NAME1": vary_case(rng, name),
        "SMTP_ADDR": email_variant(rng, p) if rng.random() < 0.8 else "",
        "TELF1": phone_format(rng, p.phone10) if rng.random() < 0.9 else "",
        "STRAS": f"{p.street_no} {p.street}",
        "ORT01": p.city.upper(),
        "PSTLZ": p.postcode,
        "LAND1": rng.choice(COUNTRY_VARIANTS["erp_a"]),
        "STCD1": p.tax_id or "",
        "AEDAT": ts(rng, "%Y%m%d"),
    }


def erp_b_row(rng: random.Random, p: Person, rid: str) -> dict[str, Any]:
    return {
        "CUST_ACCOUNT_ID": rid,
        "PARTY_NAME": vary_case(
            rng, f"{vary_first(rng, p.first)} {typo(rng, p.last) if rng.random() < 0.08 else p.last}"
        ),
        "EMAIL_ADDRESS": email_variant(rng, p) if rng.random() < 0.85 else "",
        "PHONE_NUMBER": phone_format(rng, p.phone10) if rng.random() < 0.85 else "",
        "ADDRESS1": f"#{p.street_no}, {p.street}"
        if rng.random() < 0.5
        else f"{p.street_no} {p.street.upper()}",
        "CITY": p.city,
        "POSTAL_CODE": f"{p.postcode[:3]} {p.postcode[3:]}" if rng.random() < 0.3 else p.postcode,
        "COUNTRY": rng.choice(COUNTRY_VARIANTS["erp_b"]),
        "TAX_REFERENCE": (p.tax_id[:5] + "-" + p.tax_id[5:])
        if p.tax_id and rng.random() < 0.5
        else (p.tax_id or ""),
        "LAST_UPDATE_DATE": ts(rng, "%d-%m-%Y"),
    }


def crm_row(rng: random.Random, p: Person, rid: str, consent_flip: bool) -> dict[str, Any]:
    consent = dict(p.consent)
    if consent_flip:
        consent["email"] = "no" if consent["email"] == "yes" else "yes"
    return {
        "ContactId": rid,
        "FirstName": vary_case(rng, vary_first(rng, p.first)),
        "LastName": vary_case(rng, p.last),
        "Email": email_variant(rng, p),
        "MobilePhone": phone_format(rng, p.phone10) if rng.random() < 0.8 else "",
        "Street": f"{p.street_no} {p.street}" if rng.random() < 0.7 else "",
        "City": p.city,
        "PostalCode": p.postcode,
        "Country": rng.choice(COUNTRY_VARIANTS["crm"]),
        "EmailOptIn": consent_text(rng, consent["email"], "bool"),
        "SmsOptIn": consent_text(rng, consent["sms"], "bool"),
        "WhatsAppOptIn": consent_text(rng, consent["whatsapp"], "bool"),
        "ModifiedOn": ts(rng, "%Y-%m-%dT%H:%M:%S"),
    }


def web_row(rng: random.Random, p: Person, rid: str) -> dict[str, Any]:
    return {
        "signup_id": rid,
        "full_name": vary_case(rng, f"{vary_first(rng, p.first)} {p.last}"),
        "email": email_variant(rng, p),
        "phone": phone_format(rng, p.phone10) if rng.random() < 0.6 else None,
        "city": p.city.lower() if rng.random() < 0.5 else p.city,
        "pincode": p.postcode if rng.random() < 0.7 else None,
        "marketing_opt_in": consent_text(rng, p.consent["email"], "yn") if rng.random() < 0.9 else None,
        "created_at": ts(rng, "%Y-%m-%d %H:%M:%S"),
    }


def pos_row(rng: random.Random, p: Person, rid: str) -> dict[str, Any]:
    return {
        "member_id": rid,
        "member_name": vary_case(rng, f"{vary_first(rng, p.first)} {p.last}"),
        "mobile": phone_format(rng, p.phone10),
        "store_pin": p.postcode,
        "store_city": p.city,
        "card_no": p.loyalty or "",
        "sms_ok": consent_text(rng, p.consent["sms"], "num"),
        "last_visit": ts(rng, "%d/%m/%Y"),
    }


def mobile_row(rng: random.Random, p: Person, rid: str) -> dict[str, Any]:
    return {
        "app_user_id": rid,
        "given_name": vary_first(rng, p.first),
        "family_name": p.last,
        "email": email_variant(rng, p) if rng.random() < 0.9 else None,
        "phone": phone_format(rng, p.phone10) if rng.random() < 0.7 else None,
        "loyalty_card": p.loyalty if p.loyalty and rng.random() < 0.6 else None,
        "push_whatsapp": consent_text(rng, p.consent["whatsapp"], "word"),
        "updated_at": ts(rng, "%Y-%m-%dT%H:%M:%S"),
    }


ROW_BUILDERS = {
    "erp_a": erp_a_row,
    "erp_b": erp_b_row,
    "web": web_row,
    "pos_loyalty": pos_row,
    "mobile": mobile_row,
}
ID_PREFIX = {"erp_a": "", "erp_b": "B", "crm": "CRM-", "web": "W", "pos_loyalty": "M", "mobile": "U"}
FILES = {
    "erp_a": "erp_a/customers_001.csv",
    "erp_b": "erp_b/customers_001.csv",
    "crm": "crm/contacts_001.csv",
    "web": "web/signups_001.json",
    "pos_loyalty": "pos_loyalty/members_001.csv",
    "mobile": "mobile/profiles_001.json",
}


def junk_rows(rng: random.Random, source: str, start: int, count: int) -> list[dict[str, Any]]:
    """Placeholder / walk-in / test records with shared fake identifiers (no real person)."""
    rows = []
    for k in range(count):
        rid = f"{ID_PREFIX[source]}{start + k}"
        fake_phone = rng.choice(["9999999999", "0000000000", "1234567890"])
        if source == "pos_loyalty":
            rows.append(
                {
                    "member_id": rid,
                    "member_name": rng.choice(["Walk In Customer", "WALK-IN", "Customer"]),
                    "mobile": fake_phone,
                    "store_pin": "",
                    "store_city": rng.choice(CITIES)[0],
                    "card_no": "",
                    "sms_ok": "",
                    "last_visit": ts(rng, "%d/%m/%Y"),
                }
            )
        elif source == "web":
            rows.append(
                {
                    "signup_id": rid,
                    "full_name": rng.choice(["test test", "Dummy User", "xxx"]),
                    "email": rng.choice(["test@test.com", "noemail@noemail.com", "abc@example.com"]),
                    "phone": fake_phone,
                    "city": None,
                    "pincode": None,
                    "marketing_opt_in": None,
                    "created_at": ts(rng, "%Y-%m-%d %H:%M:%S"),
                }
            )
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


# ----------------------------------------------------------------------------- main build
def generate(n_people: int, out: Path, seed: int) -> dict[str, int]:
    rng = random.Random(seed)
    people = make_people(rng, n_people)
    rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    truth: list[tuple[str, str]] = []
    counters = dict.fromkeys(SOURCES, 1000)

    for p in people:
        chosen = [s for s in SOURCES if rng.random() < SOURCE_PRESENCE[s]] or [rng.choice(SOURCES)]
        for source in chosen:
            copies = 2 if source in {"web", "crm"} and rng.random() < 0.08 else 1  # in-source duplicates
            for _ in range(copies):
                counters[source] += 1
                rid = f"{ID_PREFIX[source]}{counters[source]}"
                if source == "crm":
                    row = crm_row(rng, p, rid, consent_flip=rng.random() < 0.1)
                else:
                    row = ROW_BUILDERS[source](rng, p, rid)
                rows[source].append(row)
                record_id = f"{source}|{rid}"
                p.records.append(record_id)
                truth.append((record_id, p.person_id))

    for source, count in (("pos_loyalty", max(5, n_people // 100)), ("web", max(5, n_people // 150))):
        junk = junk_rows(rng, source, counters[source] + 1, count)
        counters[source] += count
        rows[source].extend(junk)
        id_col = "member_id" if source == "pos_loyalty" else "signup_id"
        truth.extend((f"{source}|{r[id_col]}", f"JUNK-{source}-{r[id_col]}") for r in junk)

    # Re-delivered rows (exact duplicates) test idempotent ingestion.
    for source in ("erp_a", "crm"):
        rows[source].extend(rng.sample(rows[source], k=min(20, len(rows[source]))))

    for source in SOURCES:
        path = out / FILES[source]
        (write_jsonl if path.suffix == ".json" else write_csv)(path, rows[source])

    write_csv(
        out / "erp_a/orders_001.csv",
        _orders(rng, rows["erp_a"], "KUNNR", "VBELN", "ERDAT", "NETWR", "%Y%m%d"),
    )
    write_csv(
        out / "erp_b/orders_001.csv",
        _orders(
            rng, rows["erp_b"], "CUST_ACCOUNT_ID", "ORDER_NUMBER", "ORDERED_DATE", "ORDER_AMOUNT", "%d-%m-%Y"
        ),
    )
    write_csv(out / "engagement/sends_001.csv", _sends(rng, rows))

    gt = out / "_ground_truth"
    gt.mkdir(parents=True, exist_ok=True)
    with (gt / "truth.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["record_id", "person_id", "household_id"])
        household = {p.person_id: p.household or "" for p in people}
        w.writerows((r, pid, household.get(pid, "")) for r, pid in sorted(set(truth)))
    labelled = _labelled_pairs(rng, people)
    write_csv(gt / "labelled_pairs.csv", labelled)
    return {
        "people": n_people,
        "records": len(set(truth)),
        "labelled_pairs": len(labelled),
        **{f"rows_{s}": len(rows[s]) for s in SOURCES},
    }


def _orders(
    rng: random.Random,
    customers: list[dict[str, Any]],
    cust_col: str,
    id_col: str,
    date_col: str,
    amount_col: str,
    fmt: str,
) -> list[dict[str, Any]]:
    out = []
    seen: set[str] = set()
    for c in customers:
        if c[cust_col] in seen:
            continue
        seen.add(c[cust_col])
        for _ in range(rng.randrange(0, 5)):
            out.append(
                {
                    id_col: f"{id_col[:2]}{len(out) + 1:08d}",
                    cust_col: c[cust_col],
                    date_col: ts(rng, fmt),
                    amount_col: f"{rng.uniform(150, 25000):.2f}" if rng.random() > 0.01 else "N/A",
                }
            )
    return out


def _sends(rng: random.Random, rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Campaign sends go to *source records*, so duplicates receive the same campaign twice."""
    id_cols = {"crm": "ContactId", "web": "signup_id", "mobile": "app_user_id", "pos_loyalty": "member_id"}
    channels = {
        "crm": ["email", "sms", "whatsapp"],
        "web": ["email"],
        "mobile": ["whatsapp", "email"],
        "pos_loyalty": ["sms"],
    }
    out = []
    for campaign in CAMPAIGNS:
        for source, id_col in id_cols.items():
            for row in rows[source]:
                if rng.random() > 0.5:
                    continue
                channel = rng.choice(channels[source])
                opened = rng.random() < 0.3
                clicked = opened and rng.random() < 0.3
                out.append(
                    {
                        "send_id": f"S{len(out) + 1:09d}",
                        "customer_source": source,
                        "customer_source_id": row[id_col],
                        "campaign": campaign,
                        "channel": channel,
                        "cost": f"{CHANNEL_COST[channel]:.2f}",
                        "sent_at": ts(rng, "%Y-%m-%dT%H:%M:%S"),
                        "opened": int(opened),
                        "clicked": int(clicked),
                        "converted": int(clicked and rng.random() < 0.2),
                    }
                )
    return out


def _labelled_pairs(rng: random.Random, people: list[Person]) -> list[dict[str, Any]]:
    """True matches (all record pairs of sampled people) plus hard non-matches."""
    pairs: list[dict[str, Any]] = []
    multi = [p for p in people if len(set(p.records)) > 1]
    for p in rng.sample(multi, k=min(len(multi), 400)):
        for a, b in combinations(sorted(set(p.records)), 2):
            pairs.append({"left_id": a, "right_id": b, "is_match": 1, "pair_type": "same_person"})
    households: dict[str, list[Person]] = defaultdict(list)
    for p in people:
        if p.household:
            households[p.household].append(p)
    for members in households.values():
        if len(members) == 2 and members[0].records and members[1].records:
            pairs.append(
                {
                    "left_id": members[0].records[0],
                    "right_id": members[1].records[0],
                    "is_match": 0,
                    "pair_type": "household",
                }
            )
    by_name: dict[tuple[str, str], list[Person]] = defaultdict(list)
    for p in people:
        by_name[(p.first, p.last)].append(p)
    for group in by_name.values():
        for a, b in list(combinations(group, 2))[:3]:
            if a.records and b.records:
                pairs.append(
                    {
                        "left_id": a.records[0],
                        "right_id": b.records[0],
                        "is_match": 0,
                        "pair_type": "same_name",
                    }
                )
    for row in pairs:  # canonical order, matching the pipeline's left_id < right_id convention
        if row["left_id"] > row["right_id"]:
            row["left_id"], row["right_id"] = row["right_id"], row["left_id"]
    return pairs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--people", type=int, default=5000)
    parser.add_argument("--out", type=Path, default=Path("local_data/landing"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    stats = generate(args.people, args.out, args.seed)
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
