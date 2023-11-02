import time
from random import uniform
from functools import partial, wraps
from typing import Callable, Optional


def retry(
    func: Callable = None,
    retries: int = 10,
    delay: int = 60,
    jitter: Optional[int] = None,
):
    if func is None:
        return partial(retry, retries=retries, delay=delay, jitter=jitter)

    @wraps(func)
    def decorator(*args, **kwargs):
        for i in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as error:
                if jitter:
                    jitter_value = uniform(-1 * jitter, jitter)
                else:
                    jitter_value = 0
                print(
                    f"Function {func.__name__} failed with error: {error}. Retrying in {round(delay+jitter_value, 2)} seconds..."
                )
                time.sleep(delay + jitter_value)
        raise Exception(f"Function {func.__name__} failed after {retries} retries.")

    return decorator
