# coding:utf-8

from neo4j import GraphDatabase
from conf.config import *
class Neo4j:
    def __init__(self):
        self.host = OASIS_NEO4J_HOST
        self.user = OASIS_NEO4J_USERNAME
        self.password = OASIS_NEO4J_PASSWORD
        self.driver = GraphDatabase.driver(self.host,auth=(self.user,self.password))

    def close(self):
        self.driver.close()

Neo4jDriver = Neo4j().driver