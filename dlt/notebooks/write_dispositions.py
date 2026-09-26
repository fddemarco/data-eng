import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import dlt
    import duckdb
    import pendulum

    from dlt.sources.rest_api import (
        RESTAPIConfig,
        check_connection,
        rest_api_resources,
        rest_api_source,
    )

    return RESTAPIConfig, dlt, duckdb, pendulum, rest_api_resources


@app.cell
def _():
    reupdated_data = [
        {
            "id": "1",
            "name": "bulbasaur",
            "size": {"weight": 6.9, "height": 0.7},
            "created_at": "2024-12-01",
            "updated_at": "2024-12-01",
        },
        {
            "id": "4",
            "name": "charmander",
            "size": {"weight": 8.5, "height": 0.6},
            "created_at": "2024-09-01",
            "updated_at": "2024-09-01",
        },
        {
            "id": "25",
            "name": "pikachu",
            "size": {"weight": 7.5, "height": 0.4},
            "created_at": "2023-06-01",
            "updated_at": "2024-12-23",
        },
    ]
    return (reupdated_data,)


@app.cell
def _(dlt, reupdated_data):
    @dlt.resource(
        name="pokemon",
        primary_key="id",
        write_disposition={"disposition": "merge"}, #, "strategy": "delete-insert"},
    )
    def append_pokemon(data,cursor_date=dlt.sources.incremental("updated_at", initial_value="2024-01-01")):
        yield data


    append_pipeline = dlt.pipeline(
        pipeline_name="append_poke_pipeline",
        destination="duckdb",
        dataset_name="pokemon_data",
        progress=dlt.progress.tqdm(colour="yellow"),
    )

    load_info = append_pipeline.run(append_pokemon(reupdated_data))#, refresh="drop_sources")
    print(load_info)
    return


@app.cell
def _(duckdb):
    with duckdb.connect("append_poke_pipeline.duckdb", read_only=True) as _conn:
        _df = _conn.sql("SELECT * FROM pokemon_data.pokemon").pl()
    _df
    return


@app.cell
def _():
    import os
    from dlt.extract import DltResource
    from dlt.sources.helpers import requests
    from dlt.sources.helpers.rest_client import RESTClient
    from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
    from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

    os.environ["SOURCES__ACCESS_TOKEN"][:5]
    return BearerTokenAuth, HeaderLinkPaginator, RESTClient


@app.cell
def _(BearerTokenAuth, HeaderLinkPaginator, RESTClient, dlt):
    @dlt.source
    def github_source(access_token: str = dlt.secrets.value):
        client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=access_token),
            paginator=HeaderLinkPaginator(),
        )

        @dlt.resource(name="issues", write_disposition="merge", primary_key="id")
        def github_issues(
            cursor_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "updated_at", initial_value="2026-01-01"
            )
        ):
            params = {
                "since": (cursor_date.last_value),
                "status": "open",
            }
            for page in client.paginate("repos/dlt-hub/dlt/issues", params=params):
                yield page

        return github_issues


    # define new dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="github_incr",
        dataset_name="gh_inc",
        destination="duckdb",
        progress=dlt.progress.tqdm(colour="yellow"),
        #refresh="drop_sources"
    )


    # run the pipeline with the new resource
    print(pipeline.run(github_source()))
    return


@app.cell
def _(duckdb):
    with duckdb.connect("github_incr.duckdb") as _conn:
        _df = _conn.sql("DESCRIBE").pl()
    _df
    return


@app.cell
def _(BearerTokenAuth, HeaderLinkPaginator, RESTClient, dlt):
    @dlt.source
    def github_issues_source(access_token: str = dlt.secrets.value):
        client = RESTClient(
            base_url="https://api.github.com",
            auth=BearerTokenAuth(token=access_token),
            paginator=HeaderLinkPaginator(),
        )

        @dlt.resource(name="issues", write_disposition="merge", primary_key="id")
        def github_issues(
            cursor_date: dlt.sources.incremental[str] = dlt.sources.incremental(
                "updated_at", initial_value="2026-01-01"
            )
        ):
            params = {
                "since": (cursor_date.last_value),
                "status": "open",
            }
            for page in client.paginate("repos/dlt-hub/dlt/issues", params=params):
                yield page

        return github_issues


    # define new dlt pipeline
    _pipeline = dlt.pipeline(
        pipeline_name="github_incr",
        dataset_name="gh_inc",
        destination="duckdb",
        progress=dlt.progress.tqdm(colour="yellow"),
        #refresh="drop_sources"
    )


    # run the pipeline with the new resource
    print(_pipeline.run(github_issues_source()))
    return


@app.cell
def _(RESTAPIConfig, dlt, pendulum, rest_api_resources):
    OWNER = 'dlt-hub'
    REPO = "dlt"

    @dlt.source(name="github")
    def github_inc_source(access_token = dlt.secrets.value):
        # Create a REST API configuration for the GitHub API
        # Use RESTAPIConfig to get autocompletion and type checking
        config: RESTAPIConfig = {
            "client": {
                "base_url": f"https://api.github.com/repos/{OWNER}/{REPO}/pulls",
                "paginator": {"type": "header_link"},
                # we add an auth config if the auth token is present
                "auth": (
                    {
                        "type": "bearer",
                        "token": access_token,
                    }
                    if access_token
                    else None
                ),
            },
            "resources": [
                {
                    "name": "comments",
                    "primary_key": "id",
                    "write_disposition": {"disposition": "append"},#, "strategy": "delete-insert"},
                    "endpoint": {
                        "path": "comments",
                        # Query parameters for the endpoint
                        "params": {
                            "per_page": 100,
                            "sort": "updated_at",
                            "direction": "desc",
                            "since": "{incremental.start_value}",
                        },
                        # For incremental to work, we need to define the cursor_path
                        # (the field that will be used to get the incremental value)
                        # and the initial value
                        "incremental": {
                            "cursor_path": "updated_at",
                            "initial_value": pendulum.today().subtract(days=30).to_iso8601_string(),
                            "row_order": "desc"
                        },
                    },
                },
            ],
        }

        yield from rest_api_resources(config)


    return (github_inc_source,)


@app.cell
def _(dlt, github_inc_source):
    _pipeline = dlt.pipeline(
        pipeline_name="github_inc",
        dataset_name="pull",
        destination="duckdb",
    )
    _load_info = _pipeline.run(github_inc_source)
    print(_load_info)
    print(_pipeline.state)
    print(_pipeline.last_trace)
    return


@app.cell
def _(duckdb):
    with duckdb.connect("github_inc.duckdb") as _conn:
        _df = _conn.sql("SELECT * FROM pull.comments").pl()
    _df
    return


if __name__ == "__main__":
    app.run()
