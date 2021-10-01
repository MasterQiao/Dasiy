package com.masterqiao.dasiy;

import static com.masterqiao.dasiy.Labels.LinkTypes.T1;
import static com.masterqiao.dasiy.Labels.LinkTypes.T2;
import static com.masterqiao.dasiy.Labels.LinkTypes.T3;
import static com.masterqiao.dasiy.Labels.NodeLabels.L1;
import static com.masterqiao.dasiy.Labels.NodeLabels.L2;
import static com.masterqiao.dasiy.Labels.NodeLabels.L3;
import static com.masterqiao.dasiy.Labels.NodeLabels.L4;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;

class RemoteNodeTest {
	private static GraphDatabaseService graphDb;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		graphDb = DatabaseConfig.getGraphDatabaseService();
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	/*	(n1:L1:L4) (n2:L1:L2) (n3:L3)
	 *  (n1)-[r1:T1]->(n3)
	 *	(n2)-[r2:T1]->(n3)
	 *	(n3)-[r3:T2]->(n2)
	 *	(n1)-[r4:T3]->(n1)
	 */

	@BeforeEach
	void setUp() throws Exception {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode(L1, L4);
			n1.setProperty("name", "n1");
			Node n2 = tx.createNode(L1, L2);
			n2.setProperty("name", "n2");
			Node n3 = tx.createNode(L3);
			n3.setProperty("name", "n3");

			Relationship r1 = n1.createRelationshipTo(n3, T1);
			r1.setProperty("name", "n1n3");
			Relationship r2 = n2.createRelationshipTo(n3, T1);
			r2.setProperty("name", "n2n3");
			Relationship r3 = n3.createRelationshipTo(n2, T2);
			r3.setProperty("name", "n3n2");
			Relationship r4 = n1.createRelationshipTo(n1, T3);
			r4.setProperty("name", "n1n1");

			tx.commit();
		}
	}

	@AfterEach
	void tearDown() throws Exception {
		try (Transaction tx = graphDb.beginTx()) {
			tx.getAllRelationships().forEach(Relationship::delete);
			tx.getAllNodes().forEach(Node::delete);
			tx.commit();
		}
	}

	@Test
	void testAddLabel() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode();
			node.addLabel(L1);
			assertTrue(node.hasLabel(L1));
			node.addLabel(L2);
			assertTrue(node.hasLabel(L1));
			assertTrue(node.hasLabel(L2));
			node.delete();
			tx.commit();
		}
	}

	@Test
	void testCreateRelationshipTo() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node1 = tx.createNode();
			Node node2 = tx.createNode();
			Relationship rel = node1.createRelationshipTo(node2, T1);
			assertNotNull(node1.getSingleRelationship(T1, Direction.OUTGOING));
			rel.delete();
			node1.delete();
			node2.delete();
			tx.commit();
		}
	}

	@Test
	void testDelete() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode();
			final long id = node.getId();
			node.delete();
			assertThrows(RuntimeException.class, () -> tx.getNodeById(id));
			tx.commit();
		}
	}

	@Test
	void testGetAllProperties() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode();
			Map<String, Object> map = new HashMap<>();
			map.put("StringValue", "Hello");
			map.put("intValue", 1);
			map.put("longValue", 2L);
			map.put("floatValue", 0.1f);
			map.put("intArr", new int[] { 1, 2, 3 });
			map.put("doubleArr", new double[] { 0.1, 0.2, 0.3 });
			LocalDate date = LocalDate.now();
			map.put("LocalDate", date);

			map.forEach((k, v) -> node.setProperty(k, v));

			Map<String, Object> props = node.getAllProperties();
			assertEquals(map.size(), props.size());
			assertEquals(map.keySet(), props.keySet());

			node.delete();
			tx.commit();
		}
	}

	@Test
	void testGetDegree() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			assertEquals(2, n1.getDegree());

			Node n2 = tx.findNode(L2, "name", "n2");
			assertEquals(2, n2.getDegree());

			Node n3 = tx.findNode(L3, "name", "n3");
			assertEquals(3, n3.getDegree());
		}
	}

	@Test
	void testGetDegreeDirection() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			assertEquals(1, n1.getDegree(Direction.INCOMING));

			Node n2 = tx.findNode(L2, "name", "n2");
			assertEquals(1, n2.getDegree(Direction.OUTGOING));

			Node n3 = tx.findNode(L3, "name", "n3");
			assertEquals(3, n3.getDegree(Direction.BOTH));
		}
	}

	@Test
	void testGetDegreeRelationshipType() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n3 = tx.findNode(L3, "name", "n3");
			assertEquals(2, n3.getDegree(T1));
			assertEquals(1, n3.getDegree(T2));
		}
	}

	@Test
	void testGetDegreeRelationshipTypeDirection() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n3 = tx.findNode(L3, "name", "n3");
			assertEquals(0, n3.getDegree(T1, Direction.OUTGOING));
			assertEquals(1, n3.getDegree(T2, Direction.OUTGOING));
		}
	}

	@Test
	void testGetId() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n3 = tx.findNode(L3, "name", "n3");
			Node node = tx.getNodeById(n3.getId());
			assertEquals("n3", node.getProperty("name"));
		}
	}

	@Test
	void testGetLabels() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			Set<String> set = new HashSet<>();
			n1.getLabels().forEach(label -> set.add(label.name()));
			assertEquals(2, set.size());
			assertTrue(set.contains(L1.name()));
			assertTrue(set.contains(L4.name()));
		}
	}

	@Test
	void testGetProperties() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode();
			Map<String, Object> map = new HashMap<>();
			map.put("StringValue", "Hello");
			map.put("intValue", 1);
			map.put("longValue", 2L);
			map.put("floatValue", 0.1f);
			map.put("intArr", new int[] { 1, 2, 3 });
			map.put("floatArr", new float[] { 0.1f, 0.2f, 0.3f });
			LocalDate date = LocalDate.now();
			map.put("LocalDate", date);

			map.forEach((k, v) -> node.setProperty(k, v));

			Map<String, Object> props = node.getProperties("intValue", "longValue", "intArr", "floatArr", "LocalDate");
			assertEquals(1, ((Number) props.get("intValue")).intValue());
			assertEquals(2L, props.get("longValue"));

			Object obj = props.get("intArr");
			assertArrayEquals(new int[] { 1, 2, 3 },
					obj.getClass().isArray() ? (int[]) obj : Ints.toArray((Collection<? extends Number>) obj));

			obj = props.get("floatArr");
			assertArrayEquals(new float[] { 0.1f, 0.2f, 0.3f },
					obj.getClass().isArray() ? (float[]) obj : Floats.toArray((Collection<? extends Number>) obj),
					1e-7f);

			assertEquals(date, props.get("LocalDate"));

			props.put("mutable", "yes");

			node.delete();
			tx.commit();
		}
	}

	@Test
	void testGetPropertyString() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode();
			node.setProperty("intValue", 1);
			assertEquals(1, ((Number) node.getProperty("intValue")).intValue());
			assertThrows(NotFoundException.class, () -> node.getProperty("Hello"));
			node.delete();
			tx.commit();
		}
	}

	@Test
	void testGetPropertyStringObject() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode();
			node.setProperty("intValue", 1);
			assertEquals(1, ((Number) node.getProperty("intValue")).intValue());
			assertEquals(1, node.getProperty("Hello", 1));
			node.delete();
			tx.commit();
		}
	}

	@Test
	void testGetPropertyKeys() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode();
			Map<String, Object> map = new HashMap<>();
			map.put("StringValue", "Hello");
			map.put("intValue", 1);
			map.put("longValue", 2L);
			map.put("floatValue", 0.1f);
			map.put("intArr", new int[] { 1, 2, 3 });
			map.put("floatArr", new float[] { 0.1f, 0.2f, 0.3f });
			LocalDate date = LocalDate.now();
			map.put("LocalDate", date);

			map.forEach((k, v) -> node.setProperty(k, v));
			Set<String> keys = new HashSet<>();
			node.getPropertyKeys().forEach(keys::add);
			assertEquals(map.keySet(), keys);
			node.delete();
			tx.commit();
		}
	}

	@Test
	void testGetRelationships() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			List<Relationship> rels = new ArrayList<>();
			n1.getRelationships().forEach(rels::add);
			assertEquals(2, rels.size());
		}
	}

	@Test
	void testGetRelationshipsDirection() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			List<Relationship> rels = new ArrayList<>();
			n1.getRelationships(Direction.INCOMING).forEach(rels::add);
			assertEquals(1, rels.size());
		}
	}

	@Test
	void testGetRelationshipsDirectionRelationshipTypeArray() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			List<Relationship> rels = new ArrayList<>();
			n1.getRelationships(Direction.INCOMING, T3).forEach(rels::add);
			assertEquals(1, rels.size());
			assertEquals("n1n1", rels.get(0).getProperty("name"));
		}
	}

	@Test
	void testGetRelationshipsRelationshipTypeArray() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			List<Relationship> rels = new ArrayList<>();
			n1.getRelationships(T3).forEach(rels::add);
			assertEquals(1, rels.size());
			assertEquals("n1n1", rels.get(0).getProperty("name"));
		}
	}

	@Test
	void testGetRelationshipTypes() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			Set<String> set = new HashSet<>();
			n1.getRelationshipTypes().forEach(relType -> set.add(relType.name()));
			assertEquals(2, set.size());
			assertTrue(set.contains(T1.name()));
			assertTrue(set.contains(T3.name()));
		}
	}

	@Test
	void testGetSingleRelationship() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			assertEquals("n1n1", n1.getSingleRelationship(T3, Direction.INCOMING).getProperty("name"));
		}
	}

	@Test
	void testHasLabel() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			assertTrue(n1.hasLabel(L4));
			assertFalse(n1.hasLabel(L2));
		}
	}

	@Test
	void testHasProperty() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			assertTrue(n1.hasProperty("name"));
			assertFalse(n1.hasProperty("id"));
		}
	}

	@Test
	void testHasRelationship() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.findNode(L1, "name", "n1");
			assertTrue(n1.hasRelationship());

			Node node = tx.createNode();
			assertFalse(node.hasRelationship());
			node.delete();

			tx.commit();
		}
	}

	@Test
	void testHasRelationshipDirection() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n3 = tx.findNode(L3, "name", "n3");
			assertTrue(n3.hasRelationship(Direction.INCOMING));
		}
	}

	@Test
	void testHasRelationshipDirectionRelationshipTypeArray() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n3 = tx.findNode(L3, "name", "n3");
			assertTrue(n3.hasRelationship(Direction.INCOMING, T1));
			assertFalse(n3.hasRelationship(Direction.OUTGOING, T1));
		}
	}

	@Test
	void testHasRelationshipRelationshipTypeArray() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n3 = tx.findNode(L3, "name", "n3");
			assertTrue(n3.hasRelationship(T1));
			assertFalse(n3.hasRelationship(T3));
		}
	}

	@Test
	void testRemoveLabel() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode(L1, L2, L3);
			assertTrue(node.hasLabel(L1));
			node.removeLabel(L1);
			assertFalse(node.hasLabel(L1));
			node.delete();
			tx.commit();
		}
	}

	@Test
	void testRemoveProperty() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode(L1, L2, L3);
			node.setProperty("intValue", 1);
			assertTrue(node.hasProperty("intValue"));
			assertEquals(1, ((Number) node.removeProperty("intValue")).intValue());
			assertFalse(node.hasProperty("intValue"));
			node.delete();
			tx.commit();
		}
	}

	@Test
	void testSetProperty() {
		testRemoveProperty();
	}

}
