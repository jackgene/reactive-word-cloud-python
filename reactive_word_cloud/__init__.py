from typing import Any, Callable, TypeVar, cast

from reactivex import Observable


In = TypeVar("In", covariant=True)
Out = TypeVar("Out", contravariant=True)


def _pipe(obs: Observable[In], op: Callable[[Observable[In]], Observable[Out]]) -> Observable[Out]:
    return obs.pipe(op)


# Add >> to reactivex.Observable
cast(Any, Observable).__rshift__ = _pipe
