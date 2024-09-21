import re
from itertools import groupby
from typing import TypeVar

import reactivex as rx
import reactivex.operators as ops
from confluent_kafka import Consumer, Message
from reactivex.observable.observable import Observable

from reactive_word_cloud.config import KafkaConfig, WordCloudConfig
from reactive_word_cloud.model import *

L = TypeVar('L')
R = TypeVar('R')

def user_input_from_kafka(config: KafkaConfig) -> Observable[SenderAndText]:
    def consume_message(consumer: Consumer) -> rx.Observable[Message]:
        msg: Message | None = consumer.poll(timeout=1.0)
        return rx.just(msg) if msg is not None else rx.empty()

    def not_error(msg: Message) -> bool:
        return not msg.error()

    def extract_chat_message(msg: Message) -> Observable[SenderAndText]:
        value: str | bytes | None = msg.value()
        sender_text: SenderAndText | None
        match value:
            case str():
                sender_text = SenderAndText.from_json(value)
            case bytes():
                sender_text = SenderAndText.from_json(value.decode('utf-8'))
            case _:
                sender_text = None
        if sender_text is not None:
            print(f'consumed: {sender_text.to_json()}')
        return rx.empty() if sender_text is None else rx.just(sender_text)

    consumer: Consumer = Consumer({
        'bootstrap.servers': ','.join(config.bootstrap_servers),
        'group.id': config.group_id,
        'enable.auto.commit': 'true' if config.enable_auto_commit else 'false',
        'auto.offset.reset': config.auto_offset_reset
    })
    consumer.subscribe(config.topic_names)
    return rx.repeat_value(consumer) \
        >> ops.concat_map(consume_message) \
        >> ops.filter(not_error) \
        >> ops.concat_map(extract_chat_message)

class WordCloudService:
    def __init__(self, config: WordCloudConfig) -> None:
        self.max_words_per_sender: int = config.max_words_per_sender
        self.min_word_len: int = config.min_word_len
        self.max_word_len: int = config.max_word_len
        self.stop_words: set[str] = set(config.stop_words)

    def normalize_text(self, sender_text: SenderAndText) -> SenderAndText:
        return SenderAndText(
            sender_text.sender,
            re.sub(r'[^\w]+', ' ', sender_text.text).strip().lower()
        )

    def split_into_words(
        self, sender_text: SenderAndText
    ) -> Observable[SenderAndWord]:
        text: str = sender_text.text
        words: list[str] = text.split(' ')
        return rx.from_iterable([
            SenderAndWord(sender_text.sender, word)
            for word in reversed(words)
        ])

    def is_valid_word(self, sender_word: SenderAndWord) -> bool:
        word: str = sender_word.word
        return self.min_word_len <= len(word) <= self.max_word_len \
            and word not in self.stop_words

    def update_words_for_sender(
        self, words_by_sender: dict[str, list[str]], sender_word: SenderAndWord
    ) -> dict[str, list[str]]:
        sender: str = sender_word.sender
        word: str = sender_word.word
        old_words: list[str] = words_by_sender.get(sender, [])
        new_words: list[str] = list(dict.fromkeys([word] + old_words))
        new_words = new_words[0:self.max_words_per_sender]
        return words_by_sender | {sender: new_words}

    def count_senders_by_word(
        self, words_by_sender: dict[str, list[str]]
    ) -> dict[str, int]:
        words: list[str] = sorted([
            word 
            for _, words in words_by_sender.items()
            for word in words
        ])
        return {word: len([*grp]) for word, grp in groupby(words)}

    def word_counts(
        self, src_msgs: Observable[SenderAndText]
    ) -> Observable[Counts]:
        return src_msgs \
            >> ops.map(self.normalize_text) \
            >> ops.concat_map(self.split_into_words) \
            >> ops.filter(self.is_valid_word) \
            >> ops.scan(self.update_words_for_sender, seed={}) \
            >> ops.map(self.count_senders_by_word) \
            >> ops.map(Counts)

    def debugging_word_counts(
        self, src_msgs: Observable[SenderAndText]
    ) -> Observable[DebuggingCounts]:
        def normalize_split_validate(
            msg: SenderAndText
        ) -> tuple[SenderAndText, str, list[tuple[str, bool]]]:
            normalized_text: str = self.normalize_text(sender_text=msg).text
            words: list[str] = normalized_text.split(' ')

            return msg, normalized_text, [(word, self.is_valid_word(SenderAndWord('', word))) for word in words]

        def aggregate(
            accum: tuple[DebuggingCounts, dict[str, list[str]]],
            next: tuple[SenderAndText, str, list[tuple[str, bool]]]
        ) -> tuple[DebuggingCounts, dict[str, list[str]]]:
            counts: DebuggingCounts = accum[0]
            old_words_by_sender: dict[str, list[str]] = accum[1]
            msg: SenderAndText = next[0]
            normalized_text: str = next[1]
            split_words: list[tuple[str, bool]] = next[2]

            extracted_words: list[ExtractedWord] = []
            extracted_word: ExtractedWord = ExtractedWord('', False, old_words_by_sender, self.count_senders_by_word(old_words_by_sender))
            for (word, is_valid) in reversed(split_words):
                if is_valid:
                    old_words: list[str] # This has to be a separate line, or PyRight gets sad
                    old_words = extracted_word.words_by_sender.get(msg.sender, [])
                    new_words: list[str] = list(dict.fromkeys([word] + old_words))[0:self.max_words_per_sender]
                    new_words_by_sender: dict[str, list[str]] = old_words_by_sender | {msg.sender: new_words}
                    counts_by_word: dict[str, int] = self.count_senders_by_word(new_words_by_sender)
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
        
        return src_msgs \
            >> ops.map(normalize_split_validate) \
            >> ops.scan(aggregate, seed=(DebuggingCounts([], {}), {})) \
            >> ops.map(left)
