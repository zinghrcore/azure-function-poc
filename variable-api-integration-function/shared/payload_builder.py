from datetime import datetime, date
from decimal import Decimal
import pandas as pd

# ---------------- helper functions ----------------

def safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except:
        return default


def safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except:
        return default

def format_value(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    elif isinstance(v, Decimal):
        return float(v)
    return v

# ---------------- PHILIPPINES ----------------

def transform_philippines(records):

    output = []

    for row in records:
        output.append({
            "EmployeeID": (int(row.get("EmployeeID", 0)) if pd.notna(row.get("EmployeeID")) else 0),
            "EmployeeCode": str(row.get("EmployeeCode", "")),
            "PayHeadID": (int(row.get("PayHeadID", 0)) if pd.notna(row.get("PayHeadID")) else 0),
            "PayheadCode": str(row.get("PayheadCode", "")),
            "PayHeadCategoryID": (int(row.get("PayHeadCategoryID", 0)) if pd.notna(row.get("PayHeadCategoryID")) else 0),
            "ApprovedDate": (pd.to_datetime(row.get("ApprovedDate")).strftime("%Y-%m-%d") if pd.notna(row.get("ApprovedDate")) else None),
            "Amount": (str(row.get("Amount", 0)) if pd.notna(row.get("Amount")) else 0.0),
            "Type": (int(row.get("Type", 1)) if pd.notna(row.get("Type")) else 1),
            "PFApplicable": (int(row.get("PFApplicable", 0)) if pd.notna(row.get("PFApplicable")) else 0),
            "SSCApplicable": (int(row.get("SSCApplicable", 0)) if pd.notna(row.get("SSCApplicable")) else 0),
            "VPFApplicable": (int(row.get("VPFApplicable", 0)) if pd.notna(row.get("VPFApplicable")) else 0),
            "TaxApplicable": (int(row.get("TaxApplicable", 1)) if pd.notna(row.get("TaxApplicable")) else 1),
            "SpotTaxApplicable": (int(row.get("SpotTaxApplicable", 1)) if pd.notna(row.get("SpotTaxApplicable")) else 1)
            # LeenPayApp is intentionally omitted from the API payload
            #"LeenPayApp": int(row.get("LeenPayApp", 0)) if pd.notna(row.get("LeenPayApp")) else 0
        })

    return output

# ---------------- INDIA ----------------
def transform_india(records):

    output = []

    for row in records:
        output.append({
            "Amount": float(row.get("Amount", 0)) if pd.notna(row.get("Amount")) else 0.0,
            "ApprovedDate": (pd.to_datetime(row.get("ApprovedDate")).strftime("%Y-%m-%d") if pd.notna(row.get("ApprovedDate")) else None),
            "ESICApplicable":int(row.get("ESICApplicable",0))  if pd.notna(row.get("ESICApplicable")) else 0,
            "EmployeeCode": str(row.get("EmployeeCode", "")),
            "EmployeeID": int(row.get("EmployeeID", 0)) if pd.notna(row.get("EmployeeID")) else 0,
            "LWFApplicable":int(row.get("LWFApplicable",0))  if pd.notna(row.get("LWFApplicable")) else 0,
            "PFApplicable":int(row.get("PFApplicable",0))  if pd.notna(row.get("PFApplicable")) else 0,
            "PTApplicable":int(row.get("PTApplicable",0))  if pd.notna(row.get("PTApplicable")) else 0,
            "PayHeadCategoryID": int(row.get("PayHeadCategoryID", 0)) if pd.notna(row.get("PayHeadCategoryID")) else 0,
            "PayHeadID": int(row.get("PayHeadID", 0)) if pd.notna(row.get("PayHeadID")) else 0,
            "PayheadCode": str(row.get("PayHeadCode", "")),
            "SpotTaxApplicable": int(row.get("SpotTaxApplicable", 1)) if pd.notna(row.get("SpotTaxApplicable")) else 1,
            "TaxApplicable": int(row.get("TaxApplicable", 1)) if pd.notna(row.get("TaxApplicable")) else 1,
            "Type": int(row.get("Type", 1)) if pd.notna(row.get("Type")) else 1,
            "VPFApplicable":int(row.get("VPFApplicable",0))  if pd.notna(row.get("VPFApplicable")) else 0
            })

    return output

# ---------------- THAILAND ----------------
def transform_thailand(records):

    output = []

    for row in records:
        output.append({
            "EmployeeID": (int(row.get("EmployeeID", 0)) if pd.notna(row.get("EmployeeID")) else 0),
            "EmployeeCode": str(row.get("EmployeeCode", "")),
            "PayHeadID": (int(row.get("PayHeadID", 0)) if pd.notna(row.get("PayHeadID")) else 0),
            "PayheadCode": str(row.get("PayheadCode", "")),
            "PayHeadCategoryID": (int(row.get("PayHeadCategoryID", 0)) if pd.notna(row.get("PayHeadCategoryID")) else 0),
            "ApprovedDate": (pd.to_datetime(row.get("ApprovedDate")).strftime("%Y-%m-%d") if pd.notna(row.get("ApprovedDate")) else None),
            "Amount": (str(row.get("Amount", 0)) if pd.notna(row.get("Amount")) else 0.0),
            "Type": (int(row.get("Type", 1)) if pd.notna(row.get("Type")) else 1),
            "PFApplicable": (int(row.get("PFApplicable", 0)) if pd.notna(row.get("PFApplicable")) else 0),
            "SSCApplicable": (int(row.get("SSCApplicable", 0)) if pd.notna(row.get("SSCApplicable")) else 0),
            "VPFApplicable": (int(row.get("VPFApplicable", 0)) if pd.notna(row.get("VPFApplicable")) else 0),
            "TaxApplicable": (int(row.get("TaxApplicable", 1)) if pd.notna(row.get("TaxApplicable")) else 1),
            "SpotTaxApplicable": (int(row.get("SpotTaxApplicable", 1)) if pd.notna(row.get("SpotTaxApplicable")) else 1)
            # LeenPayApp is intentionally omitted from the API payload
            #"LeenPayApp": int(row.get("LeenPayApp", 0)) if pd.notna(row.get("LeenPayApp")) else 0
            })

    return output

# ---------------- ROUTER ----------------
def transform_records(records, country):

    if country == "INDIA":
        return transform_india(records)

    elif country == "PHILIPPINES":
        return transform_philippines(records)
    
    elif country == "THAILAND":
        return transform_thailand(records)

    else:
        raise Exception(f"Unsupported country: {country}")
