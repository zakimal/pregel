#-*- using:utf-8 -*-
import collections
import threading

class Vertex():
    def __init__(self, id, value, neighbors):
        self.id = id
        self.value = value
        self.neighbors = neighbors
        self.incoming_messages = []
        self.outgoing_messages = []
        self.is_active = True
        self.superstep = 0

class Pregel():
    def __init__(self, vertices, num_workers):
        self.vertices = vertices
        self.num_workers = num_workers

    def run(self):
        self.partition = self.partition_vertices()
        while self.check_active():
            self.superstep()
            self.spread_messages()

    def partition_vertices(self):
        partition = collections.defaultdict(list)
        for vertex in self.vertices:
            partition[self.worker(vertex)].append(vertex)
        return partition

    def worker(self, vertex):
        return hash(vertex) % self.num_workers

    def superstep(self):
        workers = []
        for vertex_list in self.partition.values():
            worker = Worker(vertex_list)
            workers.append(worker)
            worker.start()
        for worker in workers:
            worker.join()

    def spread_messages(self):
        for vertex in self.vertices:
            vertex.superstep += 1
            vertex.incoming_messages = []
        for vertex in self.vertices:
            for (recv_vertex, msg) in vertex.outgoing_messages:
                recv_vertex.incoming_messages.append((vertex, msg))

    def check_active(self):
        return any([vertex.is_active for vertex in self.vertices])

class Worker(threading.Thread):
    def __init__(self, vertices):
        threading.Thread.__init__(self)
        self.vertices = vertices

    def run(self):
        self.superstep()

    def superstep(self):
        for vertex in self.vertices:
            if vertex.is_active:
                vertex.update()