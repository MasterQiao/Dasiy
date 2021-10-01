package com.masterqiao.dasiy;

import static com.masterqiao.dasiy.Labels.LinkTypes.T1;
import static com.masterqiao.dasiy.Labels.LinkTypes.T3;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;

class RemoteRelationshipTest {

	private static GraphDatabaseService graphDb;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		graphDb = DatabaseConfig.getGraphDatabaseService();
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
		try (Transaction tx = graphDb.beginTx()) {
			tx.getAllRelationships().forEach(Relationship::delete);
			tx.getAllNodes().forEach(Node::delete);
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
	void testGetId() {
		try (Transaction tx = graphDb.beginTx()) {
			assertThrows(NotFoundException.class, () -> tx.getRelationshipById(0));

			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);
			rel.setProperty("name", "Knows");
			assertEquals("Knows", tx.getRelationshipById(rel.getId()).getProperty("name"));
		}
	}

	@Test
	void testHasProperty() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			n1.setProperty("H", 1);
			Relationship rel = n1.createRelationshipTo(n2, T3);
			rel.setProperty("name", "Knows");
			assertEquals("Knows", tx.getRelationshipById(rel.getId()).getProperty("name"));
			assertTrue(tx.getRelationshipById(rel.getId()).hasProperty("name"));
			assertFalse(tx.getRelationshipById(rel.getId()).hasProperty("mame"));
		}
	}

	@Test
	void testGetPropertyString() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);
			rel.setProperty("name", "Knows");
			assertEquals("Knows", tx.getRelationshipById(rel.getId()).getProperty("name"));
			assertThrows(NotFoundException.class, () -> tx.getRelationshipById(rel.getId()).getProperty("mame"));
		}
	}

	@Test
	void testGetPropertyStringObject() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);
			rel.setProperty("name", "Knows");
			assertEquals("Knows", tx.getRelationshipById(rel.getId()).getProperty("name", ""));
			assertEquals("Knows", tx.getRelationshipById(rel.getId()).getProperty("mame", "Knows"));
		}
	}

	@Test
	void testSetProperty() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);
			rel.setProperty("intValue", 1);
			assertEquals(1, ((Number) rel.getProperty("intValue")).intValue());
		}
	}

	@Test
	void testRemoveProperty() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);
			rel.setProperty("intValue", 1);
			assertEquals(1, ((Number) rel.removeProperty("intValue")).intValue());
			assertFalse(tx.getRelationshipById(rel.getId()).hasProperty("intValue"));
		}
	}

	@Test
	void testGetPropertyKeys() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);
			Map<String, Object> expected = new HashMap<>();
			expected.put("StringValue", "Hello");
			expected.put("intValue", 1);
			expected.put("longValue", 2L);
			expected.put("floatValue", 0.1f);
			expected.put("intArr", new int[] { 1, 2, 3 });
			expected.put("floatArr", new float[] { 0.1f, 0.2f, 0.3f });
			LocalDate date = LocalDate.now();
			expected.put("LocalDate", date);
			expected.forEach((k, v) -> rel.setProperty(k, v));

			Set<String> actual = new HashSet<>();
			rel.getPropertyKeys().forEach(actual::add);
			assertEquals(expected.keySet(), actual);
		}
	}

	@Test
	void testGetProperties() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);

			Map<String, Object> expected = new HashMap<>();
			expected.put("StringValue", "Hello");
			expected.put("intValue", 1);
			expected.put("longValue", 2L);
			expected.put("floatValue", 0.1f);
			expected.put("intArr", new int[] { 1, 2, 3 });
			expected.put("floatArr", new float[] { 0.1f, 0.2f, 0.3f });
			LocalDate date = LocalDate.now();
			expected.put("LocalDate", date);

			expected.forEach((k, v) -> rel.setProperty(k, v));

			Map<String, Object> actual = rel.getProperties("intValue", "longValue", "intArr", "floatArr", "LocalDate");
			assertEquals(1, ((Number) actual.get("intValue")).intValue());
			assertEquals(2L, actual.get("longValue"));

			Object obj = actual.get("intArr");
			assertArrayEquals(new int[] { 1, 2, 3 },
					obj.getClass().isArray() ? (int[]) obj : Ints.toArray((Collection<? extends Number>) obj));

			obj = actual.get("floatArr");
			assertArrayEquals(new float[] { 0.1f, 0.2f, 0.3f },
					obj.getClass().isArray() ? (float[]) obj : Floats.toArray((Collection<? extends Number>) obj),
					1e-7f);

			assertEquals(date, actual.get("LocalDate"));

			actual.put("mutable", "yes");
		}
	}

	@Test
	void testGetAllProperties() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);

			Map<String, Object> expected = new HashMap<>();
			expected.put("StringValue", "Hello");
			expected.put("intValue", 1);
			expected.put("longValue", 2L);
			expected.put("floatValue", 0.1f);
			expected.put("intArr", new int[] { 1, 2, 3 });
			expected.put("floatArr", new float[] { 0.1f, 0.2f, 0.3f });
			LocalDate date = LocalDate.now();
			expected.put("LocalDate", date);

			expected.forEach((k, v) -> rel.setProperty(k, v));

			Map<String, Object> actual = rel.getAllProperties();
			assertEquals(expected.keySet(), actual.keySet());
			assertEquals(1, ((Number) actual.get("intValue")).intValue());
			assertEquals(2L, actual.get("longValue"));

			Object obj = actual.get("intArr");
			assertArrayEquals(new int[] { 1, 2, 3 },
					obj.getClass().isArray() ? (int[]) obj : Ints.toArray((Collection<? extends Number>) obj));

			obj = actual.get("floatArr");
			assertArrayEquals(new float[] { 0.1f, 0.2f, 0.3f },
					obj.getClass().isArray() ? (float[]) obj : Floats.toArray((Collection<? extends Number>) obj),
					1e-7f);

			assertEquals(date, actual.get("LocalDate"));

		}
	}

	@Test
	void testDelete() {
		try (Transaction tx = graphDb.beginTx()) {
			Node startNode = tx.createNode();
			Node endNode = tx.createNode();
			Relationship rel = startNode.createRelationshipTo(endNode, T1);
			assertEquals(1, startNode.getDegree());
			rel.delete();
			assertEquals(0, startNode.getDegree());
		}
	}

	@Test
	void testGetStartNode() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);
			assertEquals(tx.getRelationshipById(rel.getId()).getStartNode().getId(), n1.getId());
		}
	}

	@Test
	void testGetEndNode() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);
			assertEquals(tx.getRelationshipById(rel.getId()).getEndNode().getId(), n2.getId());
		}
	}

	@Test
	void testGetOtherNode() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);
			assertEquals(tx.getRelationshipById(rel.getId()).getOtherNode(n1).getId(), n2.getId());
		}
	}

	@Test
	void testGetNodes() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);
			Node[] nodes = tx.getRelationshipById(rel.getId()).getNodes();
			assertEquals(2, nodes.length);
			assertEquals(n1.getId(), nodes[0].getId());
			assertEquals(n2.getId(), nodes[1].getId());
		}
	}

	@Test
	void testGetType() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T3);
			assertEquals(T3.name(), tx.getRelationshipById(rel.getId()).getType().name());
		}
	}

	@Test
	void testIsType() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode();
			Relationship rel = n1.createRelationshipTo(n2, T1);
			assertTrue(rel.isType(T1));
			assertFalse(rel.isType(T3));
		}
	}

}
