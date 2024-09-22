import re
from itertools import groupby
from typing import TypeVar

import reactivex as rx
import reactivex.operators as ops
from reactivex.observable.observable import Observable

from reactive_word_cloud.config import WordCloudConfig
from reactive_word_cloud.model import *

L = TypeVar('L')
R = TypeVar('R')

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
