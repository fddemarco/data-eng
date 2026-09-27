import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import dlt

    return dlt, duckdb, mo


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
    #pipeline.extract(
    #    data,
    #    table_name="items",
    #)
    #pipeline.normalize()
    #load_info = pipeline.load(workers=20)
    #print(load_info)
    return data, pipeline


@app.cell
def _(data, pipeline):
    pipeline.run(data, table_name="copy") # Triggers schema export
    return


@app.cell
def _(duckdb, mo):
    with duckdb.connect("my_pipeline.duckdb") as _conn:
        _df = _conn.sql("SELECT * FROM my_pipeline_dataset._dlt_pipeline_state").pl()
        _df1 = _conn.sql("SELECT * FROM my_pipeline_dataset.copy").pl()
        _df2 = _conn.sql("SELECT * FROM my_pipeline_dataset.copy__nested").pl()

    mo.vstack([_df, _df1, _df2])
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
