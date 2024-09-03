import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import timedelta
from itertools import groupby
from typing import Dict, Iterable, List, Sequence, Set

import reactivex as rx
import reactivex.operators as ops
from confluent_kafka import Consumer, Message
from reactivex import Observable

from ..model import ChatMessage, Counts


@dataclass
class SenderAndText:
    sender: str
    text: str

@dataclass
class SenderAndWord:
    sender: str
    word: str

max_words_per_sender: int = 3
min_word_len: int = 3
max_word_len: int = 15
stop_words: Set[str] = set()

def chat_messages(kafka_conf: Dict[str, str], topic_name: str) -> Observable[ChatMessage]:
    consumer: Consumer = Consumer(kafka_conf)
    def consume_messages() -> Iterable[Message]:
        try:
            consumer.subscribe([topic_name])

            while True:
                msg: Message | None = consumer.poll(timeout=1.0)
                # consumer.commit(asynchronous=False)
                if msg is not None:
                    yield msg
        finally:
            # Close down consumer to commit final offsets.
            consumer.close()
    def not_error(msg: Message) -> bool:
        return not msg.error()
    def extract_chat_message(msg: Message) -> Observable[ChatMessage]:
        value: str | bytes | None = msg.value()
        chat_message: ChatMessage | None
        match value:
            case str():
                chat_message = ChatMessage.from_json(value)
            case bytes():
                chat_message = ChatMessage.from_json(value.decode('utf-8'))
            case bytearray() | memoryview() | None:
                chat_message = None
        if chat_message is None:
            return rx.empty()
        return rx.just(chat_message)
    return rx.from_iterable(consume_messages()) \
        >> ops.filter(not_error) \
        >> ops.concat_map(extract_chat_message)

def normalize_text(msg: ChatMessage) -> SenderAndText:
    return SenderAndText(
        msg.sender,
        re.sub(r"[^\w]+", " ", msg.text).strip().lower()
    )

def split_into_words(
    sender_text: SenderAndText
) -> Observable[SenderAndWord]:
    def split_and_reverse(text: str) -> Iterable[str]:
        return reversed(text.split(" "))
    def make_sender_word(word: str) -> SenderAndWord:
        return SenderAndWord(sender_text.sender, word)
    return rx.just(sender_text.text) \
        >> ops.flat_map(split_and_reverse) \
        >> ops.map(make_sender_word)

def is_valid_word(sender_word: SenderAndWord) -> bool:
    word: str = sender_word.word
    return min_word_len <= len(word) <= max_word_len \
        and word not in stop_words

def update_words_for_sender(
    words_by_sender: Dict[str, List[str]],
    sender_word: SenderAndWord
) -> Dict[str, List[str]]:
    old_words: List[str] = words_by_sender.get(
        sender_word.sender, []
    )
    new_words: List[str] = list(OrderedDict(
        (w, ()) for w in ([sender_word.word] + old_words)
    ).keys())

    return words_by_sender | {sender_word.sender: new_words}

def count_words(
    words_by_sender: Dict[str, List[str]]
) -> Dict[str, int]:
    words: Sequence[str] = sorted(
        [ word 
            for _, words in words_by_sender.items()
            for word in words
        ]
    )
    return { w: sum([1 for _ in g]) for w, g in groupby(words) }

def word_counts(
    chat_messages: Observable[ChatMessage]
) -> Observable[Counts]:
    return chat_messages \
        >> ops.map(normalize_text) \
        >> ops.concat_map(split_into_words) \
        >> ops.filter(is_valid_word) \
        >> ops.scan(update_words_for_sender, seed={}) \
        >> ops.map(count_words) >> ops.map(Counts)