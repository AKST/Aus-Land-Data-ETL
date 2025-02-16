from typing import TypeVar

K = TypeVar('K')
V = TypeVar('V')

def im_pop_with_default(
    dictionary: dict[K, V],
    key: K,
    or_else: V,
) -> tuple[dict[K, V], V]:
    return {
        k: v
        for k, v in dictionary.items()
        if k != key
    }, dictionary.get(key, or_else)
