import json
from dataclasses import dataclass
from typing import Self


@dataclass(frozen=True)
class SenderAndText:
    sender: str
    text: str

    @classmethod
    def from_json(cls, serialized: str) -> Self | None:
        try:
            data: dict[str, str] = json.loads(serialized)
            sender: str | None = data.get('s')
            text: str | None = data.get('t')
            if sender is None or text is None: return None
            return cls(sender=sender, text=text)
        except:
            return None

    def to_json(self) -> str:
        return json.dumps({'s': self.sender, 't': self.text})

    def __str__(self):
        return f'{self.sender}: {self.text}'


@dataclass(frozen=True)
class SenderAndWord:
    sender: str
    word: str


@dataclass(frozen=True)
class Counts:
    counts_by_word: dict[str, int]

    def to_json(self) -> str:
        return json.dumps({'countsByWord': self.counts_by_word})


@dataclass(frozen=True)
class ExtractedWord:
    word: str
    is_valid: bool
    words_by_sender: dict[str, list[str]]
    counts_by_word: dict[str, int]

    def to_json(self) -> str:
        return json.dumps(
            {
                'word': self.word,
                'isValid': self.is_valid,
                'wordsBySender': self.words_by_sender,
                'countsByWord': self.counts_by_word
            }
        )
    

@dataclass(frozen=True)
class Event:
    chat_message: SenderAndText
    normalized_text: str
    words: list[ExtractedWord]

    def to_json(self) -> str:
        return f'{{"chatMessage":{self.chat_message.to_json()},"normalizedText":{json.dumps(self.normalized_text)},"words":[{",".join([word.to_json() for word in self.words])}]}}'


@dataclass(frozen=True)
class DebuggingCounts:
    history: list[Event]
    counts_by_word: dict[str, int]

    def to_json(self) -> str:
        return f'{{"history":[{",".join([event.to_json() for event in self.history])}],"countsByWord":{json.dumps(self.counts_by_word)}}}'
