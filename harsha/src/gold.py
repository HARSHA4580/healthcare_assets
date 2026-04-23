from pyspark import pipelines as dp
from pyspark.sql.functions import count, sum

# =========================
# 1. No-show analysis
# =========================
@dp.materialized_view()
def gold_noshow():
    appointments = spark.read.table("silver_appointments")
    return appointments.groupBy("No-show") \
        .agg(count("*").alias("total"))

# =========================
# 2. Revenue per patient
# =========================
@dp.materialized_view()
def gold_revenue():
    billing = spark.read.table("silver_billing")
    return billing.groupBy("PatientId") \
        .agg(sum("amount").alias("total_revenue"))

# =========================
# 3. Department analysis (JOIN)
# =========================
@dp.materialized_view()
def gold_department_analysis():
    appointments = spark.read.table("silver_appointments")
    doctors = spark.read.table("silver_doctors")
    departments = spark.read.table("silver_departments")
    
    df = appointments \
        .join(doctors, "doctor_id") \
        .join(departments, "department_id")
    
    return df.groupBy("department_name") \
        .agg(count("*").alias("total_appointments"))
