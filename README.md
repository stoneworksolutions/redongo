## Synopsis

redongo is a utility written in Python that allows us to enqueue bulk saves to mongo through redis, it offers serialization for django objects and multiple Mongo connection and collections simultaneous usage. It's also horizontally scalable.

## Code Example

Server:

python server.py

Client

```
import redongo
redongo.save(objects_to_save, mongo_host, mongo_collection, bulk_size, ttl)
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
