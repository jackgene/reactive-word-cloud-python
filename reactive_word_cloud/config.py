from dataclasses import dataclass
from typing import Any, Literal, Self

import dacite


class AbstractConfig:
    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Self:
        return dacite.from_dict(data_class=cls, data=raw)


@dataclass
class WordCloudConfig(AbstractConfig):
    max_words_per_sender: int
    min_word_len: int
    max_word_len: int
    stop_words: list[str]


@dataclass
class KafkaConfig(AbstractConfig):
    topic_names: list[str]
    bootstrap_servers: list[str]
    group_id: str
    enable_auto_commit: bool
    auto_offset_reset: Literal['earliest'] | Literal['latest']


@dataclass
class WebSocketsConfig(AbstractConfig):
    url: str


@dataclass
class HttpConfig(AbstractConfig):
    port: int
