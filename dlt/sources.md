---
Available dlt core sources:
---
filesystem: Reads files in s3, gs or azure buckets using fsspec and provides convenience resources for chunked reading of various file formats
rest_api: Generic API Source
sql_database: Source that loads tables form any SQLAlchemy supported database, supports batching requests and incremental loads.
---
Available dlt single file templates:
---
arrow: The Arrow Pipeline Template will show how to load and transform arrow tables.
context_rest_api: Template for building a `dlt` pipeline to ingest data from a REST API.
dataframe: The DataFrame Pipeline Template will show how to load and transform pandas dataframes.
debug: The Debug Pipeline Template will load a column with each datatype to your destination.
default: The Intro Pipeline Template contains the example from the docs intro page
fruitshop: The Default Pipeline Template provides a simple starting point for your dlt pipeline with locally generated data
github_api: The Github API templates provides a starting point to read data from REST APIs with REST Client
requests: The Requests Pipeline Template provides a simple starting point for a dlt pipeline with the requests library
---
Available verified sources:
---
Looking up verified sources at https://github.com/dlt-hub/verified-sources.git...
airtable: Source that loads tables form Airtable.
asana_dlt: This source provides data extraction from the Asana platform via their API.
bing_webmaster: A source loading history of organic search traffic from Bing Webmaster API
chess: A source loading player profiles and games from chess.com api
facebook_ads: Loads campaigns, ads sets, ads, leads and insight data from Facebook Marketing API
filesystem: (Deprecated since dlt 1.0.0 in favor of core source of the same name) Please run dlt init filesystem <destination> --branch 0.5 to access legacy version
freshdesk: This source uses Freshdesk API and dlt to load data such as Agents, Companies, Tickets
github: Source that load github issues, pull requests and reactions for a specific repository via customizable graphql query. Loads events incrementally.
google_ads: Preliminary implementation of Google Ads pipeline.
google_analytics: Defines all the sources and resources needed for Google Analytics V4
google_sheets: Loads Google Sheets data from tabs, named and explicit ranges. Contains the main source functions.
hubspot: This is a module that provides a dlt source to retrieve data from multiple endpoints of the HubSpot API
inbox: Reads messages and attachments from e-mail inbox via IMAP protocol
jira: This source uses Jira API and dlt to load data such as Issues, Users, Workflows and Projects to the database. 
kafka: A source to extract Kafka messages.
kinesis: Reads messages from Kinesis queue.
matomo: Loads reports and raw visits data from Matomo
mongodb: Source that loads collections form any a mongo database, supports incremental loads.
mux: Loads Mux views data using https://docs.mux.com/api-reference
notion: A source that extracts data from Notion API
personio: Fetches Personio Employees, Absences, Attendances.
pg_replication: Replicates postgres tables in batch using logical decoding.
pipedrive: Highly customizable source for Pipedrive, supports endpoint addition, selection and column rename
pokemon: This source provides data extraction from an example source as a starting point for new pipelines.
rest_api: (Deprecated since dlt 1.0.0 in favor of core source of the same name) Please run dlt init rest_api <destination> --branch 0.5 to access legacy version
salesforce: Source for Salesforce depending on the simple_salesforce python package.
scraping: Scraping source
shopify_dlt: Fetches Shopify Orders and Products.
slack: Fetches Slack Conversations, History and logs.
sql_database: (Deprecated since dlt 1.0.0 in favor of core source of the same name) Please run dlt init sql_database <destination> --branch 0.5 to access legacy version
strapi: Basic strapi source
stripe_analytics: This source uses Stripe API and dlt to load data such as Customer, Subscription, Event etc. to the database. 
unstructured_data: This source converts unstructured data from a specified data resource to structured data using provided queries.
workable: This source uses Workable API and dlt to load data such as Candidates, Jobs, Events, etc. to the database.
zendesk: Defines all the sources and resources needed for ZendeskSupport, ZendeskChat and ZendeskTalk
