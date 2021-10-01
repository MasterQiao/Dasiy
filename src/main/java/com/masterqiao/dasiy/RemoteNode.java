package com.masterqiao.dasiy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteNode implements Node {
	private Logger logger = LoggerFactory.getLogger(RemoteNode.class);

	private long id;
	private Transaction tx;

	public RemoteNode(Transaction tx, long id) {
		this.tx = tx;
		this.id = id;
	}

	@Override
	public void addLabel(Label label) {
		String query = "match (n) where id(n)=$id set n:" + label.name();
		tx.run(query, Values.parameters("id", id));
	}

	@Override
	public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
		String query = "match (n), (m) where id(n)=$id and id(m)=$ido create (n)-[r:" + type.name()
				+ "]->(m) return id(r)";
		Result ret = tx.run(query, Values.parameters("id", id, "ido", otherNode.getId()));
		long idRel = ret.single().get("id(r)").asLong();
		return new RemoteRelationship(tx, idRel, this, otherNode);
	}

	@Override
	public void delete() {
		String query = "match (n) where id(n)=$id delete n";
		tx.run(query, Values.parameters("id", id));
	}

	@Override
	public Map<String, Object> getAllProperties() {
		String query = "match (n) where id(n)=$id return n";
		Result ret = tx.run(query, Values.parameters("id", id));
		return ret.single().get(0).asMap();
	}

	@Override
	public int getDegree() {
		return getDegree(Direction.BOTH);
	}

	@Override
	public int getDegree(Direction direction) {
		String query = null;
		switch (direction) {
		case BOTH:
			query = "match (n)-[r]-() where id(n)=$id return count(r)";
			break;
		case INCOMING:
			query = "match (n)<-[r]-() where id(n)=$id return count(r)";
			break;
		case OUTGOING:
			query = "match (n)-[r]->() where id(n)=$id return count(r)";
			break;
		default:
			throw new RuntimeException();
		}

		Result ret = tx.run(query, Values.parameters("id", id));
		return ret.single().get("count(r)").asInt();
	}

	@Override
	public int getDegree(RelationshipType type) {
		return getDegree(type, Direction.BOTH);
	}

	@Override
	public int getDegree(RelationshipType type, Direction direction) {
		String query = null;
		switch (direction) {
		case BOTH:
			query = "match (n)-[r]-() where id(n)=$id and type(r)=$type return count(r)";
			break;
		case INCOMING:
			query = "match (n)<-[r]-() where id(n)=$id and type(r)=$type return count(r)";
			break;
		case OUTGOING:
			query = "match (n)-[r]->() where id(n)=$id and type(r)=$type return count(r)";
			break;
		default:
			throw new RuntimeException();
		}

		Result ret = tx.run(query, Values.parameters("id", id, "type", type.name()));
		return ret.single().get("count(r)").asInt();
	}

	@Override
	public long getId() {
		return id;
	}

	@Override
	public Iterable<Label> getLabels() {
		String query = "match (n) where id(n)=$id return labels(n)";
		Value value = Values.parameters("id", id);
		logger.debug("getLabels() " + query + " " + value);
		Result ret = tx.run(query, value);
		List<Label> val = ret.single().get(0).asList(t -> Label.label(t.asString()));
		return val;
	}

	@Override
	public Map<String, Object> getProperties(String... keys) {
		Map<String, Object> map = new HashMap<String, Object>();
		if (keys.length == 0) {
			return map;
		}

		StringBuilder strBuilder = new StringBuilder();
		for (String key : keys) {
			strBuilder.append(" n.");
			strBuilder.append(key);
			strBuilder.append(" as ");
			strBuilder.append(key);
			strBuilder.append(',');
		}
		strBuilder.setLength(strBuilder.length() - 1);
		String query = "match (n) where id(n)=$id return " + strBuilder.toString();
		Result ret = tx.run(query, Values.parameters("id", id));
		map.putAll(ret.single().asMap());
		return map;
	}

	/**
	 *  This method behaves differently with the native api slightly.
	 *  It returns Long, Double and List, instead of returning
	 *  Integer, Float and Array.
	 *  Hope to resolve this gap in the future
	 */
	@Override
	public Object getProperty(String key) {
		String query = "match (n) where id(n)=$id return n." + key;
		Result ret = tx.run(query, Values.parameters("id", id));
		Object val = ret.single().get(0).asObject();
		if (val == null) {
			throw new NotFoundException("Property not found: " + key);
		}
		return val;
	}

	@Override
	public Object getProperty(String key, Object defaultValue) {
		String query = "match (n) where id(n)=$id return n." + key;
		Result ret = tx.run(query, Values.parameters("id", id));
		Object val = ret.single().get(0).asObject();
		return val != null ? val : defaultValue;
	}

	@Override
	public Iterable<String> getPropertyKeys() {
		String query = "match (n) where id(n)=$id return keys(n)";
		Result ret = tx.run(query, Values.parameters("id", id));
		return ret.single().get("keys(n)").asList(Value::asString);
	}

	@Override
	public Iterable<Relationship> getRelationships() {
		String query = "match (n)-[r]-(m) where id(n)=$id return id(n), id(r), id(m)";
		Result ret = tx.run(query, Values.parameters("id", id));
		final List<Relationship> list = new ArrayList<>();
		ret.forEachRemaining(record -> {
			long startNodeId = record.get("id(n)").asLong();
			long endNodeId = record.get("id(m)").asLong();
			Node startNode = startNodeId == id ? this : new RemoteNode(tx, startNodeId);
			Node endNode = endNodeId == id ? this : new RemoteNode(tx, endNodeId);
			list.add(new RemoteRelationship(tx, record.get("id(r)").asLong(), startNode, endNode));
		});
		return list;
	}

	@Override
	public Iterable<Relationship> getRelationships(Direction dir) {
		String query = null;
		switch (dir) {
		case BOTH:
			query = "match (n)-[r]-(m) where id(n)=$id return id(n), id(r), id(m)";
		case INCOMING:
			query = "match (n)<-[r]-(m) where id(n)=$id return id(n), id(r), id(m)";
			break;
		case OUTGOING:
			query = "match (n)-[r]->(m) where id(n)=$id return id(n), id(r), id(m)";
			break;
		default:
			throw new RuntimeException();
		}

		Result ret = tx.run(query, Values.parameters("id", id));
		final List<Relationship> list = new ArrayList<>();
		ret.forEachRemaining(record -> {
			long startNodeId = record.get("id(n)").asLong();
			long endNodeId = record.get("id(m)").asLong();
			Node startNode = startNodeId == id ? this : new RemoteNode(tx, startNodeId);
			Node endNode = endNodeId == id ? this : new RemoteNode(tx, endNodeId);
			list.add(new RemoteRelationship(tx, record.get("id(r)").asLong(), startNode, endNode));
		});

		return list;
	}

	@Override
	public Iterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
		String query = null;
		switch (direction) {
		case BOTH:
			query = "match (n)-[r]-(m) where id(n)=$id and type(r) in $types return id(n), id(r), id(m)";
			break;
		case INCOMING:
			query = "match (n)<-[r]-(m) where id(n)=$id and type(r) in $types return id(n), id(r), id(m)";
			break;
		case OUTGOING:
			query = "match (n)-[r]->(m) where id(n)=$id and type(r) in $types return id(n), id(r), id(m)";
			break;
		default:
			throw new RuntimeException();
		}
		List<String> typeNames = new ArrayList<>();
		for (RelationshipType type : types) {
			typeNames.add(type.name());
		}
		Result ret = tx.run(query, Values.parameters("id", id, "types", typeNames));
		final List<Relationship> list = new ArrayList<>();
		ret.forEachRemaining(record -> {
			long startNodeId = record.get("id(n)").asLong();
			long endNodeId = record.get("id(m)").asLong();
			Node startNode = startNodeId == id ? this : new RemoteNode(tx, startNodeId);
			Node endNode = endNodeId == id ? this : new RemoteNode(tx, endNodeId);
			list.add(new RemoteRelationship(tx, record.get("id(r)").asLong(), startNode, endNode));
		});

		return list;
	}

	@Override
	public Iterable<Relationship> getRelationships(RelationshipType... types) {
		return getRelationships(Direction.BOTH, types);
	}

	@Override
	public Iterable<RelationshipType> getRelationshipTypes() {
		String query = "match (n)-[r]-(m) where id(n)=$id return distinct type(r)";
		Result ret = tx.run(query, Values.parameters("id", id));
		final Set<RelationshipType> set = new HashSet<>();
		ret.forEachRemaining(record -> {
			String type = record.get("type(r)").asString();
			set.add(RelationshipType.withName(type));
		});
		return set;
	}

	@Override
	public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
		Iterator<Relationship> it = getRelationships(dir, type).iterator();
		if (it.hasNext()) {
			Relationship rel = it.next();
			if (it.hasNext()) {
				throw new RuntimeException("More than one relationship found");
			}
			return rel;
		} else {
			return null;
		}
	}

	@Override
	public boolean hasLabel(Label label) {
		String query = "match (n) where id(n)=$id return $label in labels(n)";
		Result ret = tx.run(query, Values.parameters("id", id, "label", label.name()));
		return ret.single().get(0).isTrue();
	}

	@Override
	public boolean hasProperty(String key) {
		String query = "match (n) where id(n)=$id return exists(n." + key + ")";
		Result ret = tx.run(query, Values.parameters("id", id));
		return ret.single().get(0).isTrue();

	}

	@Override
	public boolean hasRelationship() {
		return hasRelationship(Direction.BOTH);
	}

	@Override
	public boolean hasRelationship(Direction dir) {
		String query = null;
		switch (dir) {
		case BOTH:
			query = "match (n)-[r]-() where id(n)=$id return count(r)>0";
			break;
		case INCOMING:
			query = "match (n)<-[r]-() where id(n)=$id return count(r)>0";
			break;
		case OUTGOING:
			query = "match (n)-[r]->() where id(n)=$id return count(r)>0";
			break;
		default:
			throw new RuntimeException();
		}
		Value value = Values.parameters("id", id);
		logger.debug("hasRelationship() " + query + " " + value.toString());
		Result ret = tx.run(query, value);
		return ret.single().get(0).isTrue();
	}

	@Override
	public boolean hasRelationship(Direction direction, RelationshipType... types) {
		String query = null;
		switch (direction) {
		case BOTH:
			query = "match (n)-[r]-() where id(n)=$id and type(r) in $types return count(r)>0";
			break;
		case INCOMING:
			query = "match (n)<-[r]-() where id(n)=$id and type(r) in $types return count(r)>0";
			break;
		case OUTGOING:
			query = "match (n)-[r]->() where id(n)=$id and type(r) in $types return count(r)>0";
			break;
		default:
			throw new RuntimeException();
		}
		List<String> typeNames = new ArrayList<>();
		for (RelationshipType type : types) {
			typeNames.add(type.name());
		}
		Value value = Values.parameters("id", id, "types", typeNames);
		logger.debug("hasRelationship() " + query + " " + value.toString());
		Result ret = tx.run(query, value);
		return ret.single().get(0).isTrue();
	}

	@Override
	public boolean hasRelationship(RelationshipType... types) {
		return hasRelationship(Direction.BOTH, types);
	}

	@Override
	public void removeLabel(Label label) {
		String query = "match (n) where id(n)=$id remove n:" + label.name();
		tx.run(query, Values.parameters("id", id));
	}

	@Override
	public Object removeProperty(String key) {
		Object oldObj = getProperty(key, null);
		if (oldObj != null) {
			String query = "match (n) where id(n)=$id remove n." + key;
			tx.run(query, Values.parameters("id", id));
		}
		return oldObj;
	}

	@Override
	public void setProperty(String key, Object value) {
		String query = "match (n) where id(n)=$id set n." + key + "=$value";
		tx.run(query, Values.parameters("id", id, "value", value));
	}

	@Override
	public String toString() {
		return "Node[" + id + "]";
	}

}
