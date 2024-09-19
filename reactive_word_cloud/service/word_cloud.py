import re
from itertools import groupby
from typing import Dict, Iterable, List, Sequence, Set, TypeVar

import reactivex as rx
import reactivex.operators as ops
from confluent_kafka import Consumer, Message
from reactivex.observable.observable import Observable

from reactive_word_cloud.model import *

L = TypeVar('L')
R = TypeVar('R')

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

def user_input(kafka_conf: Dict[str, str], topic_name: str) -> Observable[SenderAndText]:
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

    consumer: Consumer = Consumer(kafka_conf)
    consumer.subscribe([topic_name])
    return rx.repeat_value(consumer) \
        >> ops.concat_map(consume_message) \
        >> ops.filter(not_error) \
        >> ops.concat_map(extract_chat_message)

def normalize_text(sender_text: SenderAndText) -> SenderAndText:
    return SenderAndText(
        sender_text.sender,
        re.sub(r'[^\w]+', ' ', sender_text.text).strip().lower()
    )

def split_into_words(
    sender_text: SenderAndText
) -> Observable[SenderAndWord]:
    text: str = sender_text.text
    split_reversed: Iterable[str] = reversed(text.split(' '))
    sender_words: Iterable[SenderAndWord] = [
        SenderAndWord(sender_text.sender, word)
        for word in split_reversed
    ]
    return rx.from_iterable(sender_words)

def is_valid_word(sender_word: SenderAndWord) -> bool:
    word: str = sender_word.word
    return min_word_len <= len(word) <= max_word_len \
        and word not in stop_words

def update_words_for_sender(
    words_by_sender: Dict[str, List[str]],
    sender_word: SenderAndWord
) -> Dict[str, List[str]]:
    sender: str = sender_word.sender
    word: str = sender_word.word
    old_words: List[str] = words_by_sender.get(sender, [])
    new_words: List[str] = list(dict.fromkeys([word] + old_words))
    new_words = new_words[0:max_words_per_sender]
    return words_by_sender | {sender: new_words}

def count_senders_by_word(
    words_by_sender: Dict[str, List[str]]
) -> Dict[str, int]:
    words: Sequence[str] = sorted([
        word 
        for _, words in words_by_sender.items()
        for word in words
    ])
    return {word: len([*grp]) for word, grp in groupby(words)}

def word_counts(
    src_msgs: Observable[SenderAndText]
) -> Observable[Counts]:
    return src_msgs \
        >> ops.map(normalize_text) \
        >> ops.concat_map(split_into_words) \
        >> ops.filter(is_valid_word) \
        >> ops.scan(update_words_for_sender, seed={}) \
        >> ops.map(count_senders_by_word) \
        >> ops.map(Counts)

def debugging_word_counts(
    src_msgs: Observable[SenderAndText]
) -> Observable[DebuggingCounts]:
    def normalize_split_validate(
        msg: SenderAndText
    ) -> tuple[SenderAndText, str, Sequence[tuple[str, bool]]]:
        normalized_text: str = normalize_text(sender_text=msg).text
        words: List[str] = normalized_text.split(' ')

        return (msg, normalized_text, [(word, is_valid_word(SenderAndWord('', word))) for word in words])

    def aggregate(
        accum: tuple[DebuggingCounts, Dict[str, List[str]]],
        next: tuple[SenderAndText, str, Sequence[tuple[str, bool]]]
    ) -> tuple[DebuggingCounts, Dict[str, List[str]]]:
        counts: DebuggingCounts = accum[0]
        old_words_by_sender: Dict[str, List[str]] = accum[1]
        msg: SenderAndText = next[0]
        normalized_text: str = next[1]
        split_words: Sequence[tuple[str, bool]] = next[2]

        extracted_words: List[ExtractedWord] = []
        extracted_word: ExtractedWord = ExtractedWord('', False, old_words_by_sender, count_senders_by_word(old_words_by_sender))
        for (word, is_valid) in reversed(split_words):
            if is_valid:
                old_words: List[str] # This has to be a separate line, or PyRight gets sad
                old_words = extracted_word.words_by_sender.get(msg.sender, [])
                new_words: List[str] = list(dict.fromkeys([word] + old_words))[0:max_words_per_sender]
                new_words_by_sender: Dict[str, List[str]] = old_words_by_sender | {msg.sender: new_words}
                counts_by_word: Dict[str, int] = count_senders_by_word(new_words_by_sender)
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
