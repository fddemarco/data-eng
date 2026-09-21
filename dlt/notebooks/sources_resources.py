import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    from dlt.sources.helpers import requests
    from dlt.sources.helpers.rest_client import RESTClient
    from dlt.sources.helpers.rest_client.auth import BearerTokenAuth


    OWNER = "dlt-hub"


    @dlt.source
    def github(access_token = dlt.secrets.value):
        client = RESTClient(
                base_url="https://api.github.com", auth=BearerTokenAuth(token=access_token)
        )

        @dlt.resource(table_name="dlt_hub_repos")
        def dlt_hub_repos():
            url = f"https://api.github.com/orgs/{OWNER}/repos"
            response = requests.get(url)
            yield response.json()


        @dlt.transformer(table_name="issue_comments")
        def add_issue_comments(repos):
            for repo in repos:
                repo_name = repo["name"]
                url = f"https://api.github.com/repos/{OWNER}/{repo_name}/issues/comments"
                for page in client.paginate(
                    url, params={"per_page": 100}
                ):
                    yield page
                    break
                response = requests.get(url)
                yield response.json()

        @dlt.resource(table_name="dlt_issue_comments")
        def dlt_issue_comments():
            for page in client.paginate(
                f"repos/{OWNER}/dlt/issues/comments", params={"per_page": 100}
            ):
                yield page
                break
        return dlt_hub_repos, dlt_issue_comments, dlt_hub_repos | add_issue_comments

    return dlt, github


@app.cell
def _(dlt, github):
    pipeline = dlt.pipeline(pipeline_name="github_pipeline", destination="duckdb", dataset_name="github_repos", dev_mode=True)
    pipeline.run(github)
    return (pipeline,)


@app.cell
def _(pipeline):
    import polars as pl

    df = pl.from_arrow(pipeline.dataset().table("issue_comments").arrow())
    return df, pl


@app.cell
def _(df, pl):
    df.filter(pl.col("id") == 1162734897)
    return


if __name__ == "__main__":
    app.run()
