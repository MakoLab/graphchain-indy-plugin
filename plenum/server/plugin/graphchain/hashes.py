from hashlib import sha256

from rdflib import BNode, Graph


class InterwovenHashCalculator:

    _HASH_SIZE = 256
    _BLANK_NODE_SUBJECT_NAME = "Magic_S"
    _BLANK_NODE_OBJECT_NAME = "Magic_O"
    _MOD_OPERAND = (2 ** _HASH_SIZE)

    def calculate_hash(self, graph: Graph) -> str:
        graph_hash = 0

        for s, p, o in graph:
            triple_hash = self._calculate_triple_hash(s, p, o)
            graph_hash += self._modulo_hash(triple_hash)

            if self._is_bnode(s):
                linked_hash = \
                    self._calculate_hash_for_triples_linked_by_subject(graph, s)
                graph_hash += self._modulo_hash(linked_hash)

            if self._is_bnode(o):
                linked_hash = self._calculate_hash_for_triples_linked_by_object(graph, s)
                graph_hash += self._modulo_hash(linked_hash)

        return "{0:x}".format(graph_hash)

    def _calculate_triple_hash(self, s, p, o):
        encoded_triple = self._encode_triple(s, p, o)

        triple_hash = sha256(encoded_triple).hexdigest()
        triple_hash = int(triple_hash, base=16)

        return triple_hash

    def _calculate_hash_for_triples_linked_by_subject(self, graph, resource):
        partial_hash = 0

        linked_triples = graph.triples((None, None, resource))
        for s, p, o in linked_triples:
            partial_hash += self._modulo_hash(
                self._calculate_triple_hash(s, p, o))

        return partial_hash

    def _calculate_hash_for_triples_linked_by_object(self, graph, resource):
        partial_hash = 0

        linked_triples = graph.triples((resource, None, None))
        for s, p, o in linked_triples:
            partial_hash += self._modulo_hash(
                self._calculate_triple_hash(s, p, o))

        return partial_hash

    def _encode_triple(self, s, p, o):
        if self._is_bnode(s):
            s_encoded = self._BLANK_NODE_SUBJECT_NAME
        else:
            s_encoded = s.n3()

        p_encoded = p.n3()

        if self._is_bnode(o):
            o_encoded = self._BLANK_NODE_OBJECT_NAME
        else:
            o_encoded = o.n3()

        return (s_encoded + p_encoded + o_encoded).encode()

    def _modulo_hash(self, a_hash):
        return a_hash % self._MOD_OPERAND

    @staticmethod
    def _is_bnode(resource):
        return isinstance(resource, BNode)
