import re
from collections import OrderedDict
from dataclasses import dataclass
from itertools import groupby
from typing import Dict, Iterable, List, Sequence, Set, TypeVar

import reactivex as rx
import reactivex.operators as ops
from confluent_kafka import Consumer, Message
from reactivex.abc.observer import ObserverBase
from reactivex.abc.scheduler import SchedulerBase
from reactivex.disposable.disposable import DisposableBase
from reactivex.observable.observable import Observable

from reactive_word_cloud.model import *

L = TypeVar('L')
R = TypeVar('R')

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
stop_words: Set[str] = {
    'about',
    'above',
    'after',
    'again',
    'against',
    'all',
    'and',
    'any',
    'are',
    'because',
    'been',
    'before',
    'being',
    'below',
    'between',
    'both',
    'but',
    'can',
    'did',
    'does',
    'doing',
    'down',
    'during',
    'each',
    'few',
    'for',
    'from',
    'further',
    'had',
    'has',
    'have',
    'having',
    'her',
    'here',
    'hers',
    'herself',
    'him',
    'himself',
    'his',
    'how',
    'into',
    'its',
    'itself',
    'just',
    'me',
    'more',
    'most',
    'myself',
    'nor',
    'not',
    'now',
    'off',
    'once',
    'only',
    'other',
    'our',
    'ours',
    'ourselves',
    'out',
    'over',
    'own',
    'same',
    'she',
    'should',
    'some',
    'such',
    'than',
    'that',
    'the',
    'their',
    'theirs',
    'them',
    'themselves',
    'then',
    'there',
    'these',
    'they',
    'this',
    'those',
    'through',
    'too',
    'under',
    'until',
    'very',
    'was',
    'were',
    'what',
    'when',
    'where',
    'which',
    'while',
    'who',
    'whom',
    'why',
    'will',
    'with',
    'you',
    'your',
    'yours',
    'yourself',
    'yourselves'
}

def chat_messages(kafka_conf: Dict[str, str], topic_name: str) -> Observable[ChatMessage]:
    consumer: Consumer = Consumer(kafka_conf)
    def consume_messages(observer: ObserverBase[Message], scheduler: SchedulerBase | None) -> DisposableBase:
        try:
            consumer.subscribe([topic_name])

            while True:
                msg: Message | None = consumer.poll(timeout=1.0)
                # consumer.commit(asynchronous=False)
                if msg is not None:
                    print(f'consuming: {msg.value()}')
                    observer.on_next(msg)
        except Exception as e:
            observer.on_error(e)
        finally:
            # consumer.close()
            observer.on_completed()
        if not isinstance(observer, DisposableBase):
            raise
        return observer

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
            case _:
                chat_message = None
        if chat_message is None:
            return rx.empty()
        return rx.just(chat_message)
    return rx.create(consume_messages) \
        >> ops.filter(not_error) \
        >> ops.concat_map(extract_chat_message)

def normalize_text(msg: ChatMessage) -> SenderAndText:
    return SenderAndText(
        msg.sender,
        re.sub(r'[^\w]+', ' ', msg.text).strip().lower()
    )

def split_into_words(
    sender_text: SenderAndText
) -> Observable[SenderAndWord]:
    def split_and_reverse(text: str) -> Iterable[str]:
        return reversed(text.split(' '))
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

def debugging_word_counts(
    chat_messages: Observable[ChatMessage]
) -> Observable[DebuggingCounts]:
    def normalize_split_validate(
        msg: ChatMessage
    ) -> tuple[ChatMessage, str, Sequence[tuple[str, bool]]]:
        normalized_text: str = normalize_text(msg=msg).text
        words: List[str] = normalized_text.split(' ')

        return (msg, normalized_text, [(word, is_valid_word(SenderAndWord('', word))) for word in words])

    def aggregate(
        accum: tuple[DebuggingCounts, Dict[str, List[str]]],
        next: tuple[ChatMessage, str, Sequence[tuple[str, bool]]]
    ) -> tuple[DebuggingCounts, Dict[str, List[str]]]:
        counts: DebuggingCounts = accum[0]
        old_words_by_sender: Dict[str, List[str]] = accum[1]
        msg: ChatMessage = next[0]
        normalized_text: str = next[1]
        split_words: Sequence[tuple[str, bool]] = next[2]

        extracted_words: List[ExtractedWord] = []
        extracted_word: ExtractedWord = ExtractedWord('', False, old_words_by_sender, {})
        for (word, is_valid) in reversed(split_words):
            if is_valid:
                old_words: List[str] # This has to be a separate line, or PyRight gets sad
                old_words = extracted_word.words_by_sender.get(msg.sender, [])
                new_words: List[str] = list(OrderedDict((w, ()) for w in ([word] + old_words)).keys())
                new_words_by_sender: Dict[str, List[str]] = old_words_by_sender | {msg.sender: new_words}
                counts_by_word: Dict[str, int] = count_words(new_words_by_sender)
                extracted_word = ExtractedWord(
                    word, True, new_words_by_sender, counts_by_word
                )
            else:
                extracted_word = ExtractedWord(
                    word, False, extracted_word.words_by_sender, extracted_word.counts_by_word
                )
            extracted_words.insert(0, extracted_word)

        return (
            DebuggingCounts(
                history=counts.history + [Event(msg, normalized_text, extracted_words)],
                counts_by_word={} if len(extracted_words) == 0 else extracted_words[0].counts_by_word
            ),
            {} if len(extracted_words) == 0 else extracted_words[0].words_by_sender
        )

    def left[L, R](pair: tuple[L, R]) -> L:
        return pair[0]
    
    return chat_messages \
        >> ops.map(normalize_split_validate) \
        >> ops.scan(aggregate, seed=(DebuggingCounts([], {}), {})) \
        >> ops.map(left)
