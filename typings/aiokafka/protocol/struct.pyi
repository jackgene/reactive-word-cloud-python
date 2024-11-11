from io import BytesIO
from typing import Any, ClassVar, Union
from typing_extensions import Self

class Struct:
    SCHEMA: ClassVar = ...
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...
    
    def encode(self) -> bytes:
        ...
    
    @classmethod
    def decode(cls, data: Union[BytesIO, bytes]) -> Self:
        ...
    
    def get_item(self, name: str) -> Any:
        ...
    
    def __repr__(self) -> str:
        ...
    
    def __eq__(self, other: object) -> bool:
        ...
    


