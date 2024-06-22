import pandera as pa


def create():
    return pa.DataFrameSchema(
        name="Other",
        description="Fallback schema for data that does not match any other schema.",
    )
