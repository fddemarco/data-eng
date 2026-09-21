import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import dlt
    from dlt.sources.helpers import requests


    OWNER = "dlt-hub"

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
            response = requests.get(url)
            yield response.json()

    @dlt.source
    def github_sources():
        return dlt_hub_repos, dlt_hub_repos | add_issue_comments

    return dlt, github_sources


@app.cell
def _(dlt, github_sources):
    pipeline = dlt.pipeline(pipeline_name="github_pipeline", destination="duckdb", dataset_name="github_repos", dev_mode=True)
    pipeline.run(github_sources)
    return (pipeline,)


@app.cell
def _(pipeline):
    pipeline.dataset().table("issue_comments").arrow()
    return


@app.cell
def _(pipeline):
    pipeline.dataset().table("dlt_hub_repos").arrow()
    return


if __name__ == "__main__":
    app.run()
