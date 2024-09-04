import json
from dataclasses import dataclass
from typing import Dict, List, Self, Sequence


@dataclass
class ChatMessage:
    sender: str
    recipient: str
    text: str

    @classmethod
    def from_json(cls, serialized: str) -> Self | None:
        try:
            data: Dict[str, str] = json.loads(serialized)
            sender: str | None = data.get('s')
            recipient: str | None = data.get('r')
            text: str | None = data.get('t')
            if sender is None or recipient is None or text is None: return None
            return cls(sender=data['s'], recipient=data['r'], text=data['t'])
        except:
            return None

    def to_json(self) -> str:
        return json.dumps({'s': self.sender, 'r': self.recipient, 't': self.text})

    def __str__(self):
        return f'{self.sender} to {self.recipient}: {self.text}'


@dataclass
class Counts:
    counts_by_word: Dict[str, int]

    def to_json(self) -> str:
        return json.dumps({'countsByWord': self.counts_by_word})


@dataclass
class ExtractedWord:
    word: str
    is_valid: bool
    words_by_sender: Dict[str, List[str]]
    counts_by_word: Dict[str, int]

    def to_json(self) -> str:
        return json.dumps(
            {
                'word': self.word,
                'isValid': self.is_valid,
                'wordsBySender': self.words_by_sender,
                'countsByWord': self.counts_by_word
            }
        )
    

@dataclass
class Event:
    chat_message: ChatMessage
    normalized_text: str
    words: Sequence[ExtractedWord]

    def to_json(self) -> str:
        return f'{{"chatMessage":{self.chat_message.to_json()},"normalizedText":{json.dumps(self.normalized_text)},"words":[{",".join([word.to_json() for word in self.words])}]}}'


@dataclass
class DebuggingCounts:
    history: List[Event]
    counts_by_word: Dict[str, int]

    def to_json(self) -> str:
        return f'{{"history":[{",".join([event.to_json() for event in self.history])}],"countsByWord":{json.dumps(self.counts_by_word)}}}'
