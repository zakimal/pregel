#-*- using:utf-8 -*-
from pregel import Vertex, Pregel

import time
from numpy import mat, eye, zeros, ones, linalg
import random

num_workers = 10
num_vertices = 10000

class PageRankVertex(Vertex):
    def update(self):
        if self.superstep < 100:
            self.value = 0.15 / num_vertices + 0.85 * sum([pagerank for (vertex, pagerank) in self.incoming_messages])
            outgoing_pagerank = self.value / len(self.neighbors)
            self.outgoing_messages = [(vertex, outgoing_pagerank) for vertex in self.neighbors]
        else:
            self.is_active = False

def create_edges(vertices):
    for vertex in vertices:
        vertex.neighbors = random.sample(vertices, 3)

def calc_pagerank(vertices):
    I = mat(eye(num_vertices))
    G = zeros((num_vertices, num_vertices))
    for vertex in vertices:
        num_neighbor = len(vertex.neighbors)
        for nv in vertex.neighbors:
            G[nv.id, vertex.id] = 1.0 / num_neighbor
    P = (1.0 / num_vertices) * mat(ones((num_vertices, 1)))
    return 0.15 * ((I - 0.85 * G).I) * P

def pregel_pagerank(vertices):
    p = Pregel(vertices, num_workers)
    p.run()
    return mat([vertex.value for vertex in p.vertices]).transpose()

def main():
    vertices = [PageRankVertex(j, 1.0 / num_vertices, []) for j in range(num_vertices)]
    create_edges(vertices)
    start = time.time()
    pr = calc_pagerank(vertices)
    elapsed_time = time.time() - start
    # print("PageRank: %s" % pr)
    print("elapsed %s sec for calc PageRank" % elapsed_time)
    start = time.time()
    pr_pregel = pregel_pagerank(vertices)
    elapsed_time = time.time() - start
    # print("PageRank by Pregel: %s" % pr_pregel)
    print("elapsed %s sec for Pregel PageRank" % elapsed_time)
    diff = pr_pregel-pr
    # print("Difference between the two pagerank vectors:\n%s" % diff)
    print("The norm of the difference is: %s" % linalg.norm(diff))

if __name__ == '__main__':
    main()