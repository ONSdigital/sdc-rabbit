### Unreleased

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
