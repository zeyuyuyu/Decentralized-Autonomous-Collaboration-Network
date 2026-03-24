import hashlib
from typing import List, Dict, Set
from dataclasses import dataclass
from time import time

@dataclass
class Message:
    sender: str
    value: any
    timestamp: float
    signature: str

class ByzantineConsensus:
    def __init__(self, node_id: str, private_key: str):
        self.node_id = node_id
        self.private_key = private_key
        self.messages: Dict[str, Set[Message]] = {}
        self.decided_values: Dict[str, any] = {}
        self.round = 0
        self.f = 0  # Max Byzantine nodes tolerated

    def sign_message(self, value: any) -> str:
        message = f"{self.node_id}:{value}:{time()}"
        return hashlib.sha256(
            (message + self.private_key).encode()
        ).hexdigest()

    def broadcast_value(self, value: any) -> Message:
        msg = Message(
            sender=self.node_id,
            value=value,
            timestamp=time(),
            signature=self.sign_message(value)
        )
        self._process_message(msg)
        return msg

    def receive_message(self, message: Message) -> bool:
        if not self._verify_message(message):
            return False
        return self._process_message(message)

    def _verify_message(self, message: Message) -> bool:
        # Verify message signature and timestamp
        # In production, implement proper cryptographic verification
        return True

    def _process_message(self, message: Message) -> bool:
        round_key = str(self.round)
        if round_key not in self.messages:
            self.messages[round_key] = set()
        
        self.messages[round_key].add(message)
        
        # Check if we have enough messages to reach consensus
        if len(self.messages[round_key]) >= 2 * self.f + 1:
            self._try_decide(round_key)
        return True

    def _try_decide(self, round_key: str) -> None:
        if round_key in self.decided_values:
            return

        # Count message values
        value_counts: Dict[any, int] = {}
        for msg in self.messages[round_key]:
            if msg.value not in value_counts:
                value_counts[msg.value] = 0
            value_counts[msg.value] += 1

        # Check if any value has more than 2f+1 votes
        for value, count in value_counts.items():
            if count >= 2 * self.f + 1:
                self.decided_values[round_key] = value
                self.round += 1
                break

    def get_consensus_value(self, round_key: str) -> any:
        return self.decided_values.get(round_key)

    def set_fault_tolerance(self, f: int) -> None:
        """Set the number of Byzantine nodes that can be tolerated"""
        self.f = f
