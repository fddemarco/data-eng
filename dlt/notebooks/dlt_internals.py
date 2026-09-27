import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    import dlt

    return dlt, mo


@app.cell
def _(dlt):
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline", destination="duckdb", progress="log", export_schema_path="schemas/export"
    )

    data = [
            {"id": 1},
            {"id": 2},
            {"id": 3, "nested": [{"id": 1}, {"id": 2}]},
        ]
    pipeline.extract(
        data,
        table_name="items",
    )
    pipeline.normalize()
    load_info = pipeline.load(workers=20)
    print(load_info)

    pipeline.run(data, table_name="copy") # Triggers schema export
    return (load_info,)


@app.cell
def _(load_info):
    load_info.load_packages[0].schema
    return


@app.cell
def _(mo):
    import duckdb

    with duckdb.connect("my_pipeline.duckdb") as _conn:
        _df = _conn.sql("SELECT * FROM my_pipeline_dataset.items").pl()
        _df2 = _conn.sql("SELECT * FROM my_pipeline_dataset.items__nested").pl()

    mo.vstack([_df, _df2])
    return


if __name__ == "__main__":
    app.run()
