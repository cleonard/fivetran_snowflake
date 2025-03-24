import datetime
import json
import traceback

import requests
from dotenv import load_dotenv
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
import snowflake.connector

BASE_API_URL = "https://newsapi.org/v2/everything"

# Tabula Lingua params
TL_CONF = {"include_segments": False}
TL_URL = "https://app.tabulalingua.com/v0/standard/"


def get_latest_published_at(conf):
    """Custom method to get the latest `published_at` value from our ARTICLES table in
    Snowflake. Fivetran's state object, which keeps up with the latest datetime value
    FROM which to query the source, has gotten out-of-sync occassionally and will miss
    articles.

    TODO: Handle empty responses for beginning state."""
    cnx = snowflake.connector.connect(
        user=conf["snowflakeUser"],
        password=conf["snowflakePassword"],
        account=conf["snowflakeAccount"],
        warehouse=conf["snowflakeWarehouse"],
        database=conf["snowflakeDatabase"],
        schema=conf["snowflakeSchema"],
    )

    result = cnx.cursor().execute("select max(published_at) from article").fetchone()
    return result[0].strftime("%Y-%m-%dT%H:%M:%S")


def schema(configuration: dict):
    """Defines the destination table schema. Not required as Fivetran will infer the
    schema from the data source. I'm being explicit since I'm adding custom values to
    the sync.
    """

    if not ("topic" in configuration):
        raise ValueError("Could not find 'topic' in configuration.json")

    return [
        {
            "table": "article",
            "primary_key": ["source", "published_at"],
            "columns": {
                "source": "STRING",
                "published_at": "UTC_DATETIME",
                "author": "STRING",
                "title": "STRING",
                "description": "STRING",
                "content": "STRING",
                "url": "STRING",
                # Tabula Lingua anlyses results
                "red": "FLOAT",
                "blue": "FLOAT",
                "clarity": "FLOAT",
                "insecurity": "FLOAT",
                "apathy": "FLOAT",
                "disfunction": "FLOAT",
                "regret": "FLOAT",
            },
        }
    ]


def update(configuration: dict, state: dict):
    """Required by Fivetran. Sets up parameters, iterates through topics, and passes the
    data to sync_items() to perform each operation."""
    conf = configuration

    try:
        # Set up base params for calls to data source

        now = datetime.datetime.now()

        # From datetime:
        last_pub_date = get_latest_published_at(conf)
        from_ts = state.get("to_ts")
        if not from_ts:
            delta = datetime.timedelta(days=2)
            a = (now - delta).strftime("%Y-%m-%dT%H:%M:%S")  # Two days ago
            b = last_pub_date  # Latest `published_at` from warehouse articles
            from_ts = min(a, b)
        if last_pub_date < from_ts:
            from_ts = last_pub_date

        # To datetime:
        to_ts = now.strftime("%Y-%m-%dT%H:%M:%S")
        log.fine(
            f"Now: {now}, From: {from_ts}, To: {to_ts}, Pub: {last_pub_date}")

        params = {
            "from": from_ts,
            "to": to_ts,
            "page": "1",
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": conf["pageSize"],
        }

        headers = {
            "Authorization": f"Bearer {conf['newsApiKey']}",
            "Accept": "application/json",
        }

        tl_key = conf["tabulaKey"]

        topics = json.loads(conf["topic"])
        for t in topics:
            params["q"] = t
            params["page"] = "1"
            yield from sync_items(headers, params, state, t, tl_key)

        # Update the state with the new cursor position, incremented by 1.
        new_state = {"to_ts": to_ts}
        log.fine(f"state updated, new state: {repr(new_state)}")

        # Save the progress by checkpointing the state. This is important for ensuring
        # that the sync process can resume from the correct position in case of next
        # sync or interruptions. Learn more about how and where to checkpoint by reading
        # our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        yield op.checkpoint(state=new_state)

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = (
            f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        )
        raise RuntimeError(detailed_message)


def sync_items(headers, params, state, topic, tl_key):
    more_data = True

    while more_data:
        response_page = get_api_response(BASE_API_URL, headers, params)

        log.info(f"{response_page['totalResults']} results for topic {topic})")

        if not response_page['totalResults']:
            log.info(params)

        # Process the items.
        items = response_page.get("articles", [])
        if not items:
            break  # End pagination if there are no records in response.

        # Iterate over each user in the 'items' list and yield an upsert operation.
        # The 'upsert' operation inserts the data into the destination.
        # Update the state with the 'updatedAt' timestamp of the current item.
        summary_first_item = {
            "title": items[0]["title"],
            "source": items[0]["source"],
        }

        for a in items:
            data = {
                "topic": topic,
                "source": a["source"]["name"],
                "published_at": a["publishedAt"],
                "author": a["author"],
                "title": a["title"],
                "description": a["description"],
                "content": a["content"],
                "url": a["url"],
            }

            # Call Tabula Lingua API to add linguistic analyses values to record:
            # - In prod, this should be a seperate service that connects to Snowflake
            #   and perhaps receives a trigger/webhook from this connector
            try:
                content = data.get("content").strip()
                if not content:
                    err_msg = "Content is blank or doesn't exist in NewsAPI response"
                    raise ValueError(err_msg)

                body = {"config": TL_CONF, "text": content}
                headers = {"Auth": tl_key, "accept": "application/json"}
                tl_response = requests.post(TL_URL, headers=headers, json=body)
                tl_response.raise_for_status()

                tl_data = tl_response.json()["data"]["document"]
                data["red"] = tl_data["red"]
                data["blue"] = tl_data["blue"]
                data["clarity"] = 1 - tl_data["delta"]

                rvs = tl_data["rvs"]
                data["insecurity"] = (rvs["crt"] + rvs["bnd"]) / 2
                data["apathy"] = (rvs["avr"] + rvs["sep"]) / 2
                data["disfunction"] = (rvs["det"] + rvs["iso"]) / 2
                data["regret"] = (rvs["rej"] + rvs["ign"]) / 2

            except Exception as err:
                # Log error response
                exception_class = err.__class__.__name__
                exception_message = str(err)
                stack_trace = traceback.format_exc()
                detailed_message = (
                    f"TL Error: {exception_class} - {exception_message}\n"
                    f"Stack Trace:\n{stack_trace}"
                )
                log.warning(detailed_message)

            yield op.upsert(table="article", data=data)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        yield op.checkpoint(state)

        # Determine if we should continue pagination based on the total items and the current offset.
        more_data, params = should_continue_pagination(response_page, params)


def should_continue_pagination(response_page, params):
    has_more_pages = True

    # Determine if there are more pages to continue the pagination
    current_page = int(params["page"])
    total_results = int(response_page["totalResults"])
    page_size = int(params["pageSize"])
    total_pages = (divmod(total_results, page_size))[0] + 1

    # 100 results is a temporary limit for dev API --
    # this limit can be removed if you have a paid API key
    if (
        current_page
        and total_pages
        and current_page < total_pages
        and current_page * page_size < 100
    ):
        # Increment the page number for the next request in params
        params["page"] = current_page + 1
    else:
        # End pagination if there is no more pages pending.
        has_more_pages = False

    return has_more_pages, params


def get_api_response(endpoint_path, headers, params):
    """Generic GET request for the main API data source"""
    response = requests.get(endpoint_path, headers=headers, params=params)
    response.raise_for_status()
    response_page = response.json()
    return response_page


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    """This block is just for local testing."""
    load_dotenv()

    # Load config
    with open("configuration.json") as fp:
        config = json.load(fp)

    connector.debug(configuration=config)
