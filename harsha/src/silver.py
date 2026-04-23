from pyspark import pipelines as dp

# =========================
# Load and clean bronze tables
# =========================
@dp.materialized_view()
def silver_appointments():
    return spark.read.table("bronze_appointments").dropDuplicates().na.drop()

@dp.materialized_view()
def silver_doctors():
    return spark.read.table("bronze_doctors")

@dp.materialized_view()
def silver_departments():
    return spark.read.table("bronze_departments")

@dp.materialized_view()
def silver_billing():
    return spark.read.table("bronze_billing")

@dp.materialized_view()
def silver_diagnostics():
    return spark.read.table("bronze_diagnostics")
