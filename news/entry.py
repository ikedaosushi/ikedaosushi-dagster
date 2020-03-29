from dataclasses import dataclass, asdict, field
from typing import List

@dataclass
class Entry:
    id: str
    title: str
    published: str
    url: str
    feedly_id: str = ""
    summary: str = ""
    commonTopics: List = field(default_factory=list)
    src: str = ""
    raw_html: str = ""
    og_image: str = ""

    def to_dict(self):
        return asdict(self)