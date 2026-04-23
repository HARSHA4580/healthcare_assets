from pyspark import pipelines as dp
from pyspark.sql.functions import monotonically_increasing_id, col

# =========================
# Load main dataset
# =========================
@dp.materialized_view()
def bronze_appointments():
    appointments = spark.read.csv(
        "/Volumes/workspace/healthcare_schema/health_care/KaggleV2-May-2016.csv",
        header=True,
        inferSchema=True
    )
    
    # Add synthetic doctor_id (1 to 3)
    return appointments.withColumn(
        "doctor_id",
        (monotonically_increasing_id() % 3) + 1
    )

# =========================
# Doctors table
# =========================
@dp.materialized_view()
def bronze_doctors():
    return spark.createDataFrame([
        (1, "Dr. Smith", 101),
        (2, "Dr. John", 102),
        (3, "Dr. Alice", 103)
    ], ["doctor_id", "doctor_name", "department_id"])

# =========================
# Departments table
# =========================
@dp.materialized_view()
def bronze_departments():
    return spark.createDataFrame([
        (101, "Cardiology"),
        (102, "Neurology"),
        (103, "Orthopedics")
    ], ["department_id", "department_name"])

# =========================
# Billing table
# =========================
@dp.materialized_view()
def bronze_billing():
    appointments = spark.read.table("bronze_appointments")
    return appointments.select(
        "PatientId",
        monotonically_increasing_id().alias("bill_id"),
        (col("Age") * 10).alias("amount")
    )

# =========================
# Diagnostics table
# =========================
@dp.materialized_view()
def bronze_diagnostics():
    appointments = spark.read.table("bronze_appointments")
    return appointments.select(
        "PatientId",
        monotonically_increasing_id().alias("test_id"),
        col("Gender").alias("test_name")
    )
