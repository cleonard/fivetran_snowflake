"""The base_config.json file has basic configuration. This Python script creates a full
configuration.json file by adding values from the .env file. The .env file also provides
actual env vars for local testing."""

import json

with open("base_config.json") as fp:
    config = json.load(fp)


def snake_to_camel(key):
    """SOME_API_KEY -> someApiKey"""
    words = key.split("_")
    for i in range(len(words)):
        if i == 0:
            words[i] = words[i].lower()
        else:
            words[i] = words[i].title()
    return "".join(words)


with open(".env") as fp:
    for line in fp.readlines():
        line = line.strip()
        key, value = line.split("=")
        key_camel = snake_to_camel(key)
        config[key_camel] = value

with open("configuration.json", "w") as fp:
    json.dump(config, fp)
