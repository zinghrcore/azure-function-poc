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
                        # "DOJ": convert_to_date_string(row.get("DOJ")),
                        "absent_deduction_applicable": safe_int(row.get("Absentdeductionapplicable", 0)),
                        "country_id": safe_int(row.get("CountryId", 0)),
                        "employee_code": str(row.get("EmployeeCode", "")),
                        "employee_id": safe_int(row.get("EmployeeID", 0)),
                        "exemption": safe_int(row.get("Exemption", 0)),
                        "eff_date": str(row.get("EffDate", ""))[:10],
                        "from_date": str(row.get("from_date", ""))[:10],
                        "gender": safe_int(row.get("Gender", 0)),
                        "income_tax_applicable": safe_int(row.get("Incometaxapplicable", 0)),
                        "leave_encash_applicable": safe_int(row.get("Leaveencashapplicable", 1)),
                        "ot_applicable": safe_int(row.get("Otapplicable", 1)),
                        "monthly_rate": float(row.get("MonthlyRate", 0)),
                        "pag_ibig_hdmf_employee": safe_int(row.get("Pagibighdmfemployee", 0)),
                        "pag_ibig_hdmf_employer": safe_int(row.get("Pagibighdmfemployer", 0)),
                        "payhead_category_id": safe_int(row.get("PayHeadCategoryID", 0)),
                        "payhead_code": str(row.get("PayheadCode", "")).lower(),
                        "payhead_id": safe_int(row.get("PayheadId", 0)),
                        "ot_arrear_applicable": 1,
                        "fin_year": "2026-2026",
                        "philhealth_employee": safe_int(row.get("Philhealthemployee", 0)),
                        "philhealth_employer": safe_int(row.get("Philhealthemployer", 0)),
                        "sss_ecr_contri": safe_int(row.get("Sssecercontri", 0)),
                        "sss_employee": safe_int(row.get("Sssemployee", 0)),
                        "sss_employer": safe_int(row.get("Sssemployer", 0)),
                        "voluntary_employee_contribution_app": safe_int(row.get("Voluntaryemployeecontributionapp", 0)),
                        "w_tax": safe_int(row.get("Wtax", 0)),
                        "workers_investment_and_savings_program_app": safe_int(row.get("Workersinvestmentandsavingsprogramapp", 0)),
                        "yearly_rate": float(row.get("YearlyRate", 0))
        })

    return output

# ---------------- INDIA ----------------
def transform_india(records):

    output = []

    for row in records:
        output.append({
                        # "DOJ": "2026-01-01",
                        "absent_deduction_applicable": safe_int(row.get("Absentdeductionapplicable", 1)),
                        "country_id": safe_int(row.get("CountryId", 0)),
                        "employee_code": str(row.get("EmployeeCode", "")),  
                        "employee_id": safe_int(row.get("EmployeeID", 0)),
                        "eff_date": str(row.get("EffDate", ""))[:10],
                        "exemption": safe_int(row.get("Exemption", 1)),
                        "from_date": datetime.now().strftime("%Y-%m-%d"),
                        "gender": safe_int(row.get("Gender", 1)),
                        "income_tax_applicable": safe_int(row.get("TaxApp", 1)),
                        "leave_encash_applicable": safe_int(row.get("LeenPayApp", 1)),
                        "ot_applicable": safe_int(row.get("Otapplicable", 1)),
                        "monthly_rate": round(float(row.get("MonthlyRate", 0.0)), 2),
                        "payhead_category_id": safe_int(row.get("PayHeadCategoryID", 1)),
                        "payhead_code": str(row.get("PayheadCode", "")).lower(),
                        "workers_investment_and_savings_program_app": safe_int(row.get("Workersinvestmentandsavingsprogramapp", 0)),
                        "voluntary_employee_contribution_app": safe_int(row.get("Voluntaryemployeecontributionapp", 0)),
                        "ot_arrear_applicable": safe_int(row.get("OtArrearApplicable", 0)),
                        "fin_year":"2026-2027",
                        "payhead_id": safe_int(row.get("PayheadId", 1)),
                        "pf_employee": safe_int(row.get("Pagibighdmfemployee", 1)),
                        "pf_employer": safe_int(row.get("Pagibighdmfemployer", 1)),
                        "ssc_employee": safe_int(row.get("Sssemployee", 1)),
                        "ssc_employer": safe_int(row.get("Sssemployer", 1)),
                        "vpf_employee": safe_int(row.get("Vpfemployeeapplicable", 1)),
                        "vpf_employer": safe_int(row.get("Vpfemployerapplicable", 1)),
                        "w_tax": safe_int(row.get("Wtax", 1)),
                        "yearly_rate": float(row.get("YearlyRate", 0.0))         
            })

    return output

# ---------------- THAILAND ----------------
def transform_thailand(records):

    output = []

    for row in records:
        output.append({
                        # "DOJ": "2026-01-01",
                        "absent_deduction_applicable": safe_int(row.get("Absentdeductionapplicable", 1)),
                        "country_id": safe_int(row.get("CountryId", 0)),
                        "employee_code": str(row.get("EmployeeCode", "")),  
                        "employee_id": safe_int(row.get("EmployeeID", 0)),
                        "eff_date": str(row.get("EffDate", ""))[:10],
                        "exemption": safe_int(row.get("Exemption", 1)),
                        "from_date": datetime.now().strftime("%Y-%m-%d"),
                        "gender": safe_int(row.get("Gender", 1)),
                        "income_tax_applicable": safe_int(row.get("TaxApp", 1)),
                        "leave_encash_applicable": safe_int(row.get("LeenPayApp", 1)),
                        "ot_applicable": safe_int(row.get("Otapplicable", 1)),
                        "monthly_rate": round(float(row.get("MonthlyRate", 0.0)), 2),
                        "payhead_category_id": safe_int(row.get("PayHeadCategoryID", 1)),
                        "payhead_code": str(row.get("PayheadCode", "")).lower(),
                        "workers_investment_and_savings_program_app": safe_int(row.get("Workersinvestmentandsavingsprogramapp", 0)),
                        "voluntary_employee_contribution_app": safe_int(row.get("Voluntaryemployeecontributionapp", 0)),
                        "ot_arrear_applicable": safe_int(row.get("OtArrearApplicable", 0)),
                        "fin_year":"2026-2026",
                        "payhead_id": safe_int(row.get("PayheadId", 1)),
                        "pf_employee": safe_int(row.get("Pagibighdmfemployee", 1)),
                        "pf_employer": safe_int(row.get("Pagibighdmfemployer", 1)),
                        "ssc_employee": safe_int(row.get("Sssemployee", 1)),
                        "ssc_employer": safe_int(row.get("Sssemployer", 1)),
                        "vpf_employee": safe_int(row.get("Vpfemployeeapplicable", 1)),
                        "vpf_employer": safe_int(row.get("Vpfemployerapplicable", 1)),
                        "w_tax": safe_int(row.get("Wtax", 1)),
                        "yearly_rate": float(row.get("YearlyRate", 0.0))    
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
