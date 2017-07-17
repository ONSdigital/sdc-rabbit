class BadMessageError(Exception):
    """Raise if a message is broken in some way that means it will never
    succesfully process.

    """
    pass


class QuarantinableError(Exception):
    """An error occured, but the data may not be incorrect. Quarantine, and
    process offline.

    """
    pass


class RetryableError(Exception):
    """A retryable error is apparently transient and may be due to temporary
    network issues or misconfiguration, but the message is valid and should
    be retried.

    """
    pass


class PublishMessageError(Exception):
    """An attempt to publish a message failed.

    """
    pass
