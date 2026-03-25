import time
from typing import List, Dict, Set
import hashlib
import random

class ConsensusNode:
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = set(peers)
        self.current_leader = None
        self.term = 0
        self.votes_received = set()
        self.state = 'follower'
        self.last_heartbeat = time.time()
        self.heartbeat_timeout = random.uniform(1.5, 3.0)
        self.proposed_value = None
        self.committed_values = []
        
    def start_election(self):
        self.state = 'candidate'
        self.term += 1
        self.votes_received = {self.node_id}
        self.current_leader = None
        return {'type': 'vote_request', 'term': self.term, 'candidate_id': self.node_id}

    def handle_vote_request(self, message: Dict) -> Dict:
        if message['term'] >= self.term:
            self.term = message['term']
            self.state = 'follower'
            self.current_leader = None
            return {'type': 'vote_response', 'term': self.term, 'voter_id': self.node_id, 'granted': True}
        return {'type': 'vote_response', 'term': self.term, 'voter_id': self.node_id, 'granted': False}

    def handle_vote_response(self, message: Dict) -> None:
        if message['term'] == self.term and message['granted']:
            self.votes_received.add(message['voter_id'])
            if len(self.votes_received) > len(self.peers) / 2:
                self.state = 'leader'
                self.current_leader = self.node_id
                
    def propose_value(self, value: str) -> Dict:
        if self.state != 'leader':
            return {'type': 'error', 'message': 'Not the leader'}
        self.proposed_value = value
        return {'type': 'proposal', 'term': self.term, 'leader_id': self.node_id, 'value': value}

    def handle_proposal(self, message: Dict) -> Dict:
        if message['term'] >= self.term:
            self.term = message['term']
            self.current_leader = message['leader_id']
            self.state = 'follower'
            self.last_heartbeat = time.time()
            return {'type': 'ack', 'term': self.term, 'node_id': self.node_id, 'value': message['value']}
        return {'type': 'reject', 'term': self.term, 'node_id': self.node_id}

    def check_timeout(self) -> Dict:
        if self.state != 'leader' and time.time() - self.last_heartbeat > self.heartbeat_timeout:
            return self.start_election()
        return None

    def get_consensus_hash(self) -> str:
        """Generate a hash of all committed values to verify consensus"""
        if not self.committed_values:
            return ''
        combined = ''.join(self.committed_values)
        return hashlib.sha256(combined.encode()).hexdigest()

    def commit_value(self, value: str) -> None:
        self.committed_values.append(value)
        
    def is_leader(self) -> bool:
        return self.state == 'leader'

    def get_leader(self) -> str:
        return self.current_leader

    def reset_heartbeat(self):
        self.last_heartbeat = time.time()

    def step_down(self, term: int):
        if term > self.term:
            self.term = term
            self.state = 'follower'
            self.current_leader = None
            self.votes_received.clear()