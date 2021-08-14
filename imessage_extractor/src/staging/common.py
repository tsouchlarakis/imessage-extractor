import pandas as pd
import pydoni
from ..helpers.verbosity import bold


def columns_match_expectation(df: pd.DataFrame, table_name: str, columnspec: dict) -> bool:
    """
    Make sure that there is alignment between the columns specified in staging_table_info.json
    and the actual columns in the dataframe about to be inserted.
    """
    expected_columns = sorted([k for k, v in columnspec.items()])
    actual_columns = df.columns

    for col in expected_columns:
        if col not in actual_columns:
            raise KeyError(pydoni.advanced_strip(
                f"""Column {bold(col)} defined in staging_table_info.json
                for table {bold(table_name)} but not in actual dataframe columns
                ({bold(str(df.columns))})"""))

    for col in actual_columns:
        if col not in actual_columns:
            raise KeyError(pydoni.advanced_strip(
                f"""Column {bold(col)} in actual dataframe {bold(table_name)}
                columns ({bold(str(df.columns))}) but not in staging_table_info.json"""))
