class BadMessageError(Exception):
    """Raise if a message is broken in some way that means it will never
    succesfully process.

    """
    pass


class DecryptError(Exception):
    """Can't even decrypt the message. May be corrupt or keys may be out of
    step.

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
