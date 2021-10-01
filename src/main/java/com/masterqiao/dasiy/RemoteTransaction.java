package com.masterqiao.dasiy;

import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.StringSearchMode;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteTransaction implements org.neo4j.graphdb.Transaction {
	private Logger logger = LoggerFactory.getLogger(RemoteTransaction.class);

	private Session session;
	private Transaction tx;

	public RemoteTransaction(Session session) {
		this.session = session;
		this.tx = session.beginTransaction();
	}

	@Override
	public Node createNode() {
		String query = "create (n) return id(n)";
		logger.debug(query);
		Result ret = tx.run(query);
		if (ret.hasNext()) {
			Record rec = ret.next();
			long id = rec.get("id(n)").asLong();
			return new RemoteNode(tx, id);
		}
		throw new RuntimeException("Unknown error");
	}

	@Override
	public Node createNode(Label... labels) {
		StringBuilder strBuilder = new StringBuilder();
		for (Label label : labels) {
			strBuilder.append(":");
			strBuilder.append(label.name());
		}
		String query = "create (n" + strBuilder.toString() + ") return id(n)";
		logger.debug(query);
		Result ret = tx.run(query);
		if (ret.hasNext()) {
			Record rec = ret.next();
			long id = rec.get("id(n)").asLong();
			return new RemoteNode(tx, id);
		}
		throw new RuntimeException("Unknown error");
	}

	@Override
	public Node getNodeById(long id) {
		String query = "match (n) where id(n)=$id return id(n)";
		Value value = Values.parameters("id", id);
		logger.debug("getNodeById() " + query + " " + value.toString());
		Result ret = tx.run(query, value);
		if (!ret.hasNext()) {
			throw new NotFoundException("Node(id" + ") not found.");
		}
		return new RemoteNode(tx, id);
	}

	@Override
	public Relationship getRelationshipById(long id) {
		String query = "match (n)-[r]->(m) where id(r)=$id return id(n), id(m)";
		Value value = Values.parameters("id", id);
		logger.debug("getRelationshipById() " + query + " " + value.toString());

		Result ret = tx.run(query, value);
		if (!ret.hasNext()) {
			throw new NotFoundException("Relationship(id:" + id + ") not found.");
		}
		Record record = ret.single();
		long startNodeId = record.get("id(n)").asLong();
		long endNodeId = record.get("id(m)").asLong();
		Node startNode = new RemoteNode(tx, startNodeId);
		Node endNode = endNodeId == startNodeId ? startNode : new RemoteNode(tx, endNodeId);
		return new RemoteRelationship(tx, id, startNode, endNode);
	}

	@Override
	public void commit() {
		tx.commit();
		session.close();
	}

	@Override
	public void rollback() {
		tx.rollback();
		session.close();
	}

	@Override
	public void close() {
		tx.close();
		session.close();
	}

	@Override
	public BidirectionalTraversalDescription bidirectionalTraversalDescription() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public TraversalDescription traversalDescription() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public org.neo4j.graphdb.Result execute(String query) throws QueryExecutionException {
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public org.neo4j.graphdb.Result execute(String query, Map<String, Object> parameters)
			throws QueryExecutionException {
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public Iterable<Label> getAllLabelsInUse() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public Iterable<Label> getAllLabels() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public Iterable<RelationshipType> getAllRelationshipTypes() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public Iterable<String> getAllPropertyKeys() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public ResourceIterator<Node> findNodes(Label label, String key, String template, StringSearchMode searchMode) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public ResourceIterator<Node> findNodes(Label label, Map<String, Object> propertyValues) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2,
			String key3, Object value3) {
		Objects.requireNonNull(label);
		Objects.requireNonNull(key1);
		Objects.requireNonNull(value1);
		Objects.requireNonNull(key2);
		Objects.requireNonNull(value2);
		Objects.requireNonNull(key3);
		Objects.requireNonNull(value3);
		String query = "match (n:" + label.name() + ") where n." + key1 + "=$value1 and n." + key2 + "=$value2 and n."
				+ key3 + "=$value3 return id(n)";
		Value value = Values.parameters("value1", value1, "value2", value2, "value3", value3);
		logger.debug("findNodes() " + query + " " + value.toString());
		Result ret = tx.run(query, value);
		return new NodeResourceIterator(ret);
	}

	@Override
	public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2) {
		Objects.requireNonNull(label);
		Objects.requireNonNull(key1);
		Objects.requireNonNull(value1);
		Objects.requireNonNull(key2);
		Objects.requireNonNull(value2);
		String query = "match (n:" + label.name() + ") where n." + key1 + "=$value1 and n." + key2
				+ "=$value2 return id(n)";
		Value value = Values.parameters("value1", value1, "value2", value2);
		logger.debug("findNodes() " + query + " " + value.toString());
		Result ret = tx.run(query, value);
		return new NodeResourceIterator(ret);
	}

	@Override
	public Node findNode(Label label, String key, Object value) {
		Objects.requireNonNull(label);
		Objects.requireNonNull(key);
		Objects.requireNonNull(value);
		String query = "match (n:" + label.name() + ") where n." + key + "=$val return id(n)";
		Result ret = tx.run(query, Values.parameters("val", value));
		if (ret.hasNext()) {
			long id = ret.next().get("id(n)").asLong();
			if (ret.hasNext()) {
				throw new MultipleFoundException("More than one node found");
			}
			return new RemoteNode(tx, id);
		} else {
			return null;
		}
	}

	public class NodeResourceIterator implements ResourceIterator<Node> {
		private Result result;

		public NodeResourceIterator(Result result) {
			this.result = result;
		}

		@Override
		public boolean hasNext() {
			return result.hasNext();
		}

		@Override
		public Node next() {
			long id = result.next().get("id(n)").asLong();
			return new RemoteNode(tx, id);
		}

		@Override
		public void close() {
			result = null;
		}

	}

	@Override
	public ResourceIterator<Node> findNodes(Label label, String key, Object value) {
		Objects.requireNonNull(label);
		Objects.requireNonNull(key);
		Objects.requireNonNull(value);
		String query = "match (n:" + label.name() + ") where n." + key + "=$value return id(n)";
		Result ret = tx.run(query, Values.parameters("value", value));
		return new NodeResourceIterator(ret);
	}

	@Override
	public ResourceIterator<Node> findNodes(Label label) {
		Objects.requireNonNull(label);
		String query = "match (n:" + label.name() + ") return id(n)";
		Result ret = tx.run(query);
		return new NodeResourceIterator(ret);
	}

	@Override
	public void terminate() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public ResourceIterable<Node> getAllNodes() {
		String query = "match (n) return id(n)";
		return () -> new NodeResourceIterator(tx.run(query));
	}

	public class RelationshipResourceIterator implements ResourceIterator<Relationship> {
		private Result result;

		public RelationshipResourceIterator(Result result) {
			this.result = result;
		}

		@Override
		public boolean hasNext() {
			return result.hasNext();
		}

		@Override
		public Relationship next() {
			Record rec = result.next();
			long id = rec.get("id(r)").asLong();
			Node startNode = new RemoteNode(tx, rec.get("id(startNode(r))").asLong());
			Node endNode = new RemoteNode(tx, rec.get("id(endNode(r))").asLong());
			return new RemoteRelationship(tx, id, startNode, endNode);
		}

		@Override
		public void close() {
			result = null;
		}

	}

	@Override
	public ResourceIterable<Relationship> getAllRelationships() {
		String query = "match ()-[r]-() return id(r), id(startNode(r)), id(endNode(r))";
		return () -> new RelationshipResourceIterator(tx.run(query));
	}

	@Override
	public Lock acquireWriteLock(Entity entity) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public Lock acquireReadLock(Entity entity) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public Schema schema() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

}
