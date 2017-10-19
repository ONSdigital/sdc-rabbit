### Unreleased
- Do not restart ioloop is stop method is called on consumer
- Better exception handling of TypeErrors in consumer module when the `headers` attribute was not set in a message's properties.

### 1.0.2 2017-09-07
- Bug fix release. Removes an issue where an additional keyword argument was being passed to the logger, causing an exception.

### 1.0.1 2017-09-07
- Add configurable mandatory and immediate flags for basic publish
- Add configurable delivery confirmation for channel

### 1.0.0 2017-08-30
- Stabilised API

### 0.3.4
- Remove call to ioloop.stop in reconnect handler.

### 0.3.3
- Remove call to `logging.basicConfig`

### 0.3.2
- Remove x-delivery-count checking

### 0.3.1
- Add tornado to requirements file
- Pass tx_id in call to process method

### 0.3.0
- Move the data model to inheritence instead of composition
- Add a TornadoConsumer class that uses a TornadoConnection object to connect to RabbitMQ

### 0.1.0
- Initial release
