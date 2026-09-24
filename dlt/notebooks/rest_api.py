import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    from typing import Any, Optional

    import dlt
    from dlt.common.pendulum import pendulum
    from dlt.sources.rest_api import (
        RESTAPIConfig,
        check_connection,
        rest_api_resources,
        rest_api_source,
    )

    return (
        Any,
        Optional,
        RESTAPIConfig,
        check_connection,
        dlt,
        pendulum,
        rest_api_resources,
        rest_api_source,
    )


@app.cell
def _():
    import os

    token = os.environ["GITHUB_TOKEN"]
    return (token,)


@app.cell
def _():
    import time

    def rate_limit(response, *args, **kwargs):
        time.sleep(0.2)
        return response

    return (rate_limit,)


@app.cell
def _(
    Any,
    Optional,
    RESTAPIConfig,
    dlt,
    pendulum,
    rate_limit,
    rest_api_resources,
):
    @dlt.source(name="github")
    def github_source(access_token: Optional[str] = dlt.secrets.value) -> Any:
        # Create a REST API configuration for the GitHub API
        # Use RESTAPIConfig to get autocompletion and type checking
        print("Token configured:", bool(access_token))
        print("Token prefix:", access_token[:4] + "..." if access_token else None)
    
        config: RESTAPIConfig = {
            "client": {
                "base_url": "https://api.github.com/repos/dlt-hub/dlt/",
                "paginator": {
                    "type": "json_link",
                    "next_url_path": "paging.next",
                },
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
            # The default configuration for all resources and their endpoints
            "resource_defaults": {
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {
                    "params": {
                        "per_page": 100,
                    },
                },
            },
            "resources": [
                # This is a simple resource definition,
                # that uses the endpoint path as a resource name:
                # "pulls",
                # Alternatively, you can define the endpoint as a dictionary
                # {
                #     "name": "pulls", # <- Name of the resource
                #     "endpoint": "pulls",  # <- This is the endpoint path
                # }
                # Or use a more detailed configuration:
                {
                    "name": "issues",
                    "endpoint": {
                        "path": "issues",
                        "response_actions": [rate_limit],
                        # Query parameters for the endpoint
                        "params": {
                            "sort": "updated",
                            "direction": "desc",
                            "state": "open",
                            # Define `since` as a special parameter
                            # to incrementally load data from the API.
                            # This works by getting the updated_at value
                            # from the previous response data and using this value
                            # for the `since` query parameter in the next request.
                            "since": "{incremental.start_value}",
                        },
                        # For incremental to work, we need to define the cursor_path
                        # (the field that will be used to get the incremental value)
                        # and the initial value
                        "incremental": {
                            "cursor_path": "updated_at",
                            "initial_value": pendulum.today().subtract(days=30).to_iso8601_string(),
                        },
                    },
                },
                # The following is an example of a resource that uses
                # a parent resource (`issues`) to get the `issue_number`
                # and include it in the endpoint path:
                {
                    "name": "issue_comments",
                    "endpoint": {
                        # The placeholder `{resources.issues.number}`
                        # will be replaced with the value of `number` field
                        # in the `issues` resource data
                        "path": "issues/{resources.issues.number}/comments",
                        "response_actions": [rate_limit],
                    },
                    # Include data from `id` field of the parent resource
                    # in the child data. The field name in the child data
                    # will be called `_issues_id` (_{resource_name}_{field_name})
                    "include_from_parent": ["id"],
                },
                {
                    "name": "contributors",
                    "endpoint": {
                        # The placeholder `{resources.issues.number}`
                        # will be replaced with the value of `number` field
                        # in the `issues` resource data
                        "path": "contributors",
                        "response_actions": [rate_limit],
                    },
                },
            ],
        }

        yield from rest_api_resources(config)

    return (github_source,)


@app.cell
def _(dlt, github_source, token):
    def load_github(access_token=token) -> None:
        pipeline = dlt.pipeline(
            pipeline_name="rest_api_github",
            destination='duckdb',
            dataset_name="rest_api_data",
        )

        load_info = pipeline.run(github_source(access_token))
        print(load_info)  # noqa: T201

    return (load_github,)


@app.cell
def _(check_connection, dlt, rest_api_source):
    def load_pokemon(base_url: str = "https://pokeapi.co/api/v2/") -> None:
        pipeline = dlt.pipeline(
            pipeline_name="rest_api_pokemon",
            destination='duckdb',
            dataset_name="rest_api_data",
        )

        pokemon_source = rest_api_source(
            {
                "client": {
                    "base_url": base_url,
                    # If you leave out the paginator, it will be inferred from the API:
                    # "paginator": "json_link",
                },
                "resource_defaults": {
                    "endpoint": {
                        "params": {
                            "limit": 1000,
                        },
                    },
                },
                "resources": [
                    "pokemon",
                    "berry",
                    "location",
                ],
            },
            name="pokemon",
        )

        def check_network_and_authentication() -> None:
            (can_connect, error_msg) = check_connection(
                pokemon_source,
                "pokemon",
            )
            if not can_connect:
                raise Exception("Can't connect to the endpoint!")

        check_network_and_authentication()

        load_info = pipeline.run(pokemon_source)
        print(load_info)  # noqa: T201


    return


@app.cell
def _(load_github):
    load_github()
    #load_pokemon()
    return


@app.cell
def _():
    import duckdb

    with duckdb.connect(f"rest_api_github.duckdb") as _conn:
        df = _conn.sql(f"SELECT * FROM rest_api_data.contributors").pl() # fetch_arrow_table(), show()
    df
    return (duckdb,)


@app.cell
def _(duckdb):
    with duckdb.connect(f"rest_api_github.duckdb") as _conn:
        df_issues = _conn.sql(f"SELECT * FROM rest_api_data.issues").pl() # fetch_arrow_table(), show()
    df_issues
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
