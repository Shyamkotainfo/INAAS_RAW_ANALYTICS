from pyspark.sql import functions as F
from pyspark.sql.types import (
    NumericType, StringType, DateType, TimestampType
)


class DataFrameProfiler:
    def __init__(self, df):
        self.df = df

    def run(self, sample_size: int = 5) -> dict:
        profile = {}

        # Dataset-level stats
        total_rows = self.df.count()
        profile["dataset"] = {
            "row_count": total_rows,
            "column_count": len(self.df.columns),
        }

        column_profiles = []

        for field in self.df.schema.fields:
            col_name = field.name
            col_type = field.dataType

            col_profile = {
                "column": col_name,
                "data_type": str(col_type)
            }

            # Nulls
            null_count = self.df.filter(F.col(col_name).isNull()).count()
            col_profile["null_count"] = null_count
            col_profile["null_percentage"] = (
                round((null_count / total_rows) * 100, 2)
                if total_rows > 0 else 0
            )

            # Distinct count
            col_profile["distinct_count"] = (
                self.df.select(col_name).distinct().count()
            )

            # Sample values
            samples = (
                self.df
                .select(col_name)
                .dropna()
                .limit(sample_size)
                .rdd.flatMap(lambda x: x)
                .collect()
            )
            col_profile["sample_values"] = samples

            # Min / Max
            if isinstance(col_type, (NumericType, DateType, TimestampType)):
                min_max = self.df.select(
                    F.min(col_name).alias("min"),
                    F.max(col_name).alias("max")
                ).first()
                col_profile["min"] = min_max["min"]
                col_profile["max"] = min_max["max"]

            column_profiles.append(col_profile)

        profile["columns"] = column_profiles
        return profile
