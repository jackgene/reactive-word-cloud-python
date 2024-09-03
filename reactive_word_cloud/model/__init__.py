import json
from dataclasses import dataclass
from typing import Dict, Self


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
