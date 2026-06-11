from datetime import datetime, date
from decimal import Decimal

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
                "DOJ": str(row.get("DOJ", ""))[:10],
                "from_date": str(row.get("FromDate", ""))[:10],

                "absent_deduction_applicable": safe_int(row.get("absentdeductionapplicable")),
                "country_id": safe_int(row.get("CountryId")),
                "employee_code": str(row.get("EmployeeCode", "")),
                "employee_id": safe_int(row.get("EmployeeID")),
                "exemption": safe_int(row.get("exemption")),
                "gender": safe_int(row.get("Gender")),

                "income_tax_applicable": safe_int(row.get("incometaxapplicable")),
                "leave_encash_applicable": safe_int(row.get("LeenPayApp")),
                "ot_applicable": safe_int(row.get("Otapplicable", 1)),

                "monthly_rate": safe_float(row.get("MonthlyRate")),
                "yearly_rate": safe_float(row.get("YearlyRate")),

                "payhead_category_id": safe_int(row.get("PayHeadCategoryID")),
                "payhead_code": str(row.get("PayheadCode", "")).lower(),
                "payhead_id": safe_int(row.get("PayheadId")),

                "pag_ibig_hdmf_employee": safe_int(row.get("pagibighdmfemployee")),
                "pag_ibig_hdmf_employer": safe_int(row.get("pagibighdmfemployer")),

                "philhealth_employee": safe_int(row.get("philhealthemployee")),
                "philhealth_employer": safe_int(row.get("philhealthemployer")),

                "sss_ecr_contri": safe_int(row.get("sssecercontri")),
                "sss_employee": safe_int(row.get("sssemployee")),
                "sss_employer": safe_int(row.get("sssemployer")),

                "voluntary_employee_contribution_app": safe_int(row.get("voluntaryemployeecontributionapp")),
                "workers_investment_and_savings_program_app": safe_int(row.get("workersinvestmentandsavingsprogramapp")),

                "w_tax": safe_int(row.get("wtax"))
            })

    return output


# ---------------- INDIA ----------------
def transform_india(records):

    output = []

    for row in records:
        output.append({
                "DOJ": str(row.get("DOJ", ""))[:10],
                "from_date": str(row.get("FromDate", ""))[:10],
                "employee_code": str(row.get("EmployeeCode", "")),
                "payhead_code": str(row.get("PayHead", "")).lower(), 
                "employee_id": safe_int(row.get("EmployeeID")),
                "country_id": safe_int(row.get("CountryId")),
                "gender": safe_int(row.get("Gender", 1)),
                "payhead_id": safe_int(row.get("PayHeadID")),
                "payhead_category_id": safe_int(row.get("PayHeadCategoryID")),
                "monthly_rate": safe_float(row.get("MonthlyAmount")), 
                "yearly_rate": safe_float(row.get("YearlyAmount")),  
                "ot_applicable": safe_int(row.get("IsOTApplicable", 1)),
                "income_tax_applicable": safe_int(row.get("IsTaxApplicable", 1)),
                "pf_employee": safe_int(row.get("IsPFApplicable")),
                "pt_employee": safe_int(row.get("IsPTApplicable")),
                "lwf_employee": safe_int(row.get("IsLWFApplicable")),
                "esic_employee": safe_int(row.get("IsESICApplicable")),
                "absent_deduction_applicable": safe_int(row.get("absent_deduction_applicable")),
                "leave_encash_applicable": safe_int(row.get("leave_encash_applicable", 1)),
                "exemption": safe_int(row.get("exemption")),
                "vpf_employee": safe_int(row.get("IsVPFApplicable")),  
                "vpf_employer": safe_int(row.get("IsVPFApplicable")),  
                "is_prorata": 1,
                "esic_employer": safe_int(row.get("IsESICApplicable")),
                "pf_employer": safe_int(row.get("IsPFApplicable")),
                "pt_employer": safe_int(row.get("IsPTApplicable")),
                "lwf_employer": safe_int(row.get("IsLWFApplicable")),
                "gratuity_applicable": safe_int(row.get("gratuity_applicable")),
                "esic_employee_applicable": safe_int(row.get("IsESICApplicableForEmployee")),
                "pf_employee_applicable": safe_int(row.get("IsPFApplicableForEmployee")),
                "pt_employee_applicable": safe_int(row.get("IsPTApplicableForEmployee")),
                "lwf_employee_applicable": safe_int(row.get("IsLWFApplicableForEmployee")),
                "vpf_employee_applicable": safe_int(row.get("IsVPFApplicableForEmployee")),
                "gratuity_applicable": safe_int(row.get("gratuity_applicable")),
                "gross_net_applicable": safe_int(row.get("GrossNetApp"))
            })

    return output

# ---------------- ROUTER ----------------
def transform_records(records, country):

    if country == "INDIA":
        return transform_india(records)

    elif country == "PHILIPPINES":
        return transform_philippines(records)

    else:
        raise Exception(f"Unsupported country: {country}")
    