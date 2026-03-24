import hashlib
import time
from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class ConsensusMessage:
    node_id: str
    timestamp: float
    data: Dict
    signature: str

class ByzantineConsensus:
    def __init__(self, node_id: str, private_key: str):
        self.node_id = node_id
        self.private_key = private_key
        self.peers: List[str] = []
        self.leader: Optional[str] = None
        self.view_number = 0
        self.messages: List[ConsensusMessage] = []
        self.quorum_size = 0

    def sign_message(self, data: Dict) -> str:
        message = f"{self.node_id}:{time.time()}:{str(data)}"
        return hashlib.sha256(
            (message + self.private_key).encode()
        ).hexdigest()

    def verify_message(self, message: ConsensusMessage) -> bool:
        # In production, implement proper cryptographic verification
        return len(message.signature) == 64

    def elect_leader(self) -> str:
        """Elect leader using a deterministic algorithm based on view number"""
        if not self.peers:
            return self.node_id
        all_nodes = sorted(self.peers + [self.node_id])
        leader_index = self.view_number % len(all_nodes)
        return all_nodes[leader_index]

    def start_consensus_round(self, proposal: Dict) -> bool:
        """Initiate a new consensus round"""
        self.messages.clear()
        
        if self.node_id == self.elect_leader():
            # Leader behavior
            signature = self.sign_message(proposal)
            prepare_msg = ConsensusMessage(
                node_id=self.node_id,
                timestamp=time.time(),
                data=proposal,
                signature=signature
            )
            self.broadcast_message(prepare_msg)
            return self.collect_votes(prepare_msg)
        else:
            # Follower behavior
            return self.participate_consensus()

    def collect_votes(self, proposal: ConsensusMessage) -> bool:
        """Leader collects and validates votes from peers"""
        votes = 1  # Count self vote
        timeout = time.time() + 30  # 30 second timeout
        
        while time.time() < timeout:
            for msg in self.messages:
                if self.verify_message(msg) and msg.data == proposal.data:
                    votes += 1
                    if votes >= self.quorum_size:
                        return True
            time.sleep(0.1)
        
        self.trigger_view_change()
        return False

    def participate_consensus(self) -> bool:
        """Follower participation in consensus"""
        timeout = time.time() + 30
        
        while time.time() < timeout:
            for msg in self.messages:
                if msg.node_id == self.leader and self.verify_message(msg):
                    vote = ConsensusMessage(
                        node_id=self.node_id,
                        timestamp=time.time(),
                        data=msg.data,
                        signature=self.sign_message(msg.data)
                    )
                    self.broadcast_message(vote)
                    return True
            time.sleep(0.1)
        
        self.trigger_view_change()
        return False

    def trigger_view_change(self) -> None:
        """Initiate view change when consensus fails"""
        self.view_number += 1
        self.leader = self.elect_leader()
        self.messages.clear()

    def broadcast_message(self, message: ConsensusMessage) -> None:
        """Broadcast message to all peers"""
        self.messages.append(message)
        # In production, implement actual network broadcast

    def add_peer(self, peer_id: str) -> None:
        """Add new peer to the network"""
        if peer_id not in self.peers and peer_id != self.node_id:
            self.peers.append(peer_id)
            self.quorum_size = (len(self.peers) + 1) * 2 // 3 + 1

    def remove_peer(self, peer_id: str) -> None:
        """Remove peer from the network"""
        if peer_id in self.peers:
            self.peers.remove(peer_id)
            self.quorum_size = (len(self.peers) + 1) * 2 // 3 + 1
            if peer_id == self.leader:
                self.trigger_view_change()
