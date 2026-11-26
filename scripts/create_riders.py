from masar.spark import get_spark

spark = get_spark("create-riders")

spark.sql("""
CREATE TABLE IF NOT EXISTS riders (
    rider_id STRING,
    rider_name STRING,
    city STRING,
    signup_date DATE,
    rider_rating DOUBLE
)
USING DELTA
""")

spark.sql("""
INSERT INTO riders VALUES
('R001', 'Ahmed', 'Riyadh', DATE '2026-01-10', 4.8),
('R002', 'Sara', 'Jeddah', DATE '2026-02-15', 4.9),
('R003', 'Omar', 'Dammam', DATE '2026-03-20', 4.5)
""")

spark.sql("SELECT * FROM riders").show()

spark.stop()