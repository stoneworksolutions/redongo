## Synopsis

redongo is a utility written in Python that allows us to enqueue bulk saves to mongo through redis, it offers serialization for django objects and multiple Mongo connection and collections simultaneous usage. It's also horizontally scalable.

## Code Example

Server:

```
python server.py --redis dev-redis --redisdb 2 --redisqueue TEST
```

Client

```
import redongo_client
redongo_client.set_application_settings(application_name, mongo_host, mongo_port, mongo_collection, mongo_user, mongo_password)
redongo_client.save_to_mongo(application_name, objects_to_save)
```

## Motivation

Needed a solution that allows to enqueue bulk saves to mongo

## Installation

pip install redongo

## API Reference

WIP

## Tests

WIP

## Contributors

Stonework Solutions

## License

WIP
