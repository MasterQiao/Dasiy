package com.masterqiao.dasiy;

import static com.masterqiao.dasiy.Labels.LinkTypes.T1;
import static com.masterqiao.dasiy.Labels.LinkTypes.T2;
import static com.masterqiao.dasiy.Labels.LinkTypes.T3;
import static com.masterqiao.dasiy.Labels.NodeLabels.L1;
import static com.masterqiao.dasiy.Labels.NodeLabels.L2;
import static com.masterqiao.dasiy.Labels.NodeLabels.L3;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

class RemoteTransactionTest {

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
	void testCreateNode() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode();
			node.delete();
			tx.commit();
		}
	}

	@Test
	void testCreateNodeLabelArray() {
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode(L1, L2);

			assertTrue(node.hasLabel(L1));
			assertTrue(node.hasLabel(L2));
			assertFalse(node.hasLabel(L3));
		}
	}

	@Test
	void testGetNodeById() {
		try (Transaction tx = graphDb.beginTx()) {
			assertThrows(NotFoundException.class, () -> tx.getNodeById(0));

			Node node = tx.createNode();
			long id = node.getId();
			node.setProperty("name", "Hello");
			assertEquals("Hello", tx.getNodeById(id).getProperty("name"));
		}
	}

	@Test
	void testGetRelationshipById() {
		try (Transaction tx = graphDb.beginTx()) {
			assertThrows(NotFoundException.class, () -> tx.getRelationshipById(0));

			Node n1 = tx.createNode();
			Node n2 = tx.createNode();

			Relationship r = n1.createRelationshipTo(n2, T1);
			long id = r.getId();
			r.setProperty("name", "Knows");

			assertEquals("Knows", tx.getRelationshipById(id).getProperty("name"));
		}
	}

	@Test
	void testCommit() {
		long id = 0L;
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode();
			id = node.getId();
			node.setProperty("name", "Hello");
			tx.commit();
		}
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.getNodeById(id);
			assertEquals("Hello", node.getProperty("name"));
			node.delete();
			tx.commit();
		}

	}

	@Test
	void testRollback() {
		long id = 0L;
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode();
			id = node.getId();
			tx.commit();
		}
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.getNodeById(id);
			node.setProperty("name", "Hello");
			tx.rollback();
		}
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.getNodeById(id);
			assertFalse(node.hasProperty("name"));
			node.delete();
			tx.commit();
		}
	}

	@Test
	void testClose() {
		long id = 0L;
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.createNode();
			id = node.getId();
			tx.commit();
			tx.close();
		}
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.getNodeById(id);
			node.setProperty("name", "Hello");
			tx.close();
		}
		try (Transaction tx = graphDb.beginTx()) {
			Node node = tx.getNodeById(id);
			assertFalse(node.hasProperty("name"));
			node.delete();
			tx.commit();
		}
	}

	@Test
	void testBidirectionalTraversalDescription() {
		fail("Not yet implemented");
	}

	@Test
	void testTraversalDescription() {
		fail("Not yet implemented");
	}

	@Test
	void testExecuteString() {
		fail("Not yet implemented");
	}

	@Test
	void testExecuteStringMapOfStringObject() {
		fail("Not yet implemented");
	}

	@Test
	void testGetAllLabelsInUse() {
		fail("Not yet implemented");
	}

	@Test
	void testGetAllRelationshipTypesInUse() {
		fail("Not yet implemented");
	}

	@Test
	void testGetAllLabels() {
		fail("Not yet implemented");
	}

	@Test
	void testGetAllRelationshipTypes() {
		fail("Not yet implemented");
	}

	@Test
	void testGetAllPropertyKeys() {
		fail("Not yet implemented");
	}

	@Test
	void testFindNodesLabelStringStringStringSearchMode() {
		fail("Not yet implemented");
	}

	@Test
	void testFindNodesLabelMapOfStringObject() {
		fail("Not yet implemented");
	}

	@Test
	void testFindNodesLabelStringObjectStringObjectStringObject() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode(L1);
			n1.setProperty("name", "n1");
			n1.setProperty("intVal", 1);
			n1.setProperty("doubleVal", 2.);
			n1.setProperty("intArr", new int[] { 1, 2, 3 });
			Node n2 = tx.createNode(L1);
			n2.setProperty("name", "n2");
			n2.setProperty("intVal", 1);
			n2.setProperty("doubleVal", 2.);
			n2.setProperty("intArr", new int[] { 1, 2, 3 });
			Node n3 = tx.createNode(L1);
			n3.setProperty("name", "n3");
			n3.setProperty("intVal", 3);

			Set<String> expected = new HashSet<>();
			expected.add("n1");
			expected.add("n2");
			Set<String> actual = new HashSet<>();
			tx.findNodes(L1, "intVal", 1, "doubleVal", 2., "intArr", new int[] { 1, 2, 3 })
					.forEachRemaining(node -> actual.add((String) node.getProperty("name")));
			assertEquals(expected, actual);
		}
	}

	@Test
	void testFindNodesLabelStringObjectStringObject() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode(L1);
			n1.setProperty("name", "n1");
			n1.setProperty("intVal", 1);
			n1.setProperty("doubleVal", 2.);
			n1.setProperty("intArr", new int[] { 1, 2, 3 });
			Node n2 = tx.createNode(L1);
			n2.setProperty("name", "n2");
			n2.setProperty("intVal", 1);
			n2.setProperty("doubleVal", 2.);
			n2.setProperty("intArr", new int[] { 2, 3, 4 });
			Node n3 = tx.createNode(L1);
			n3.setProperty("name", "n3");
			n3.setProperty("intVal", 3);

			Set<String> expected = new HashSet<>();
			expected.add("n1");
			expected.add("n2");
			Set<String> actual = new HashSet<>();
			tx.findNodes(L1, "intVal", 1, "doubleVal", 2.)
					.forEachRemaining(node -> actual.add((String) node.getProperty("name")));
			assertEquals(expected, actual);
		}
	}

	@Test
	void testFindNode() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode(L1);
			n1.setProperty("name", "n1");
			n1.setProperty("intVal", 1);
			n1.setProperty("intArr", new int[] { 1, 2, 3 });
			Node n2 = tx.createNode(L1);
			n2.setProperty("name", "n2");
			n2.setProperty("intVal", 1);
			n2.setProperty("intArr", new int[] { 2, 3, 4 });

			assertEquals(n1.getId(), tx.findNode(L1, "name", "n1").getId());
			assertEquals(n1.getId(), tx.findNode(L1, "intArr", new int[] { 1, 2, 3 }).getId());
			assertNull(tx.findNode(L1, "name", "Hello"));
			assertNull(tx.findNode(L2, "name", "n1"));
			assertThrows(MultipleFoundException.class, () -> tx.findNode(L1, "intVal", 1));
		}
	}

	@Test
	void testFindNodesLabelStringObject() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode(L1);
			n1.setProperty("name", "n1");
			n1.setProperty("intVal", 1);
			n1.setProperty("intArr", new int[] { 1, 2, 3 });
			Node n2 = tx.createNode(L1);
			n2.setProperty("name", "n2");
			n2.setProperty("intVal", 1);
			n2.setProperty("intArr", new int[] { 2, 3, 4 });
			Node n3 = tx.createNode(L1);
			n3.setProperty("name", "n3");
			n3.setProperty("intVal", 3);

			Set<String> expected = new HashSet<>();
			expected.add("n1");
			expected.add("n2");
			Set<String> actual = new HashSet<>();
			tx.findNodes(L1, "intVal", 1).forEachRemaining(node -> actual.add((String) node.getProperty("name")));
			assertEquals(expected, actual);
		}
	}

	@Test
	void testFindNodesLabel() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode(L1);
			n1.setProperty("name", "n1");
			n1.setProperty("intVal", 1);
			n1.setProperty("intArr", new int[] { 1, 2, 3 });
			Node n2 = tx.createNode(L1);
			n2.setProperty("name", "n2");
			n2.setProperty("intVal", 1);
			n2.setProperty("intArr", new int[] { 2, 3, 4 });
			Node n3 = tx.createNode(L2);
			n3.setProperty("name", "n3");
			n3.setProperty("intVal", 3);

			Set<String> expected = new HashSet<>();
			expected.add("n1");
			expected.add("n2");
			Set<String> actual = new HashSet<>();
			tx.findNodes(L1).forEachRemaining(node -> actual.add((String) node.getProperty("name")));
			assertEquals(expected, actual);
		}
	}

	@Test
	void testTerminate() {
		fail("Not yet implemented");
	}

	@Test
	void testGetAllNodes() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode(L2);
			Node n3 = tx.createNode(L2, L3);
			n1.setProperty("name", "n1");
			n2.setProperty("name", "n2");
			n3.setProperty("name", "n3");
			Set<String> expected = new HashSet<>();
			expected.add("n1");
			expected.add("n2");
			expected.add("n3");

			Set<String> actual = new HashSet<>();
			tx.getAllNodes().forEach(node -> actual.add((String) node.getProperty("name")));
			assertEquals(expected, actual);
		}
	}

	@Test
	void testGetAllRelationships() {
		try (Transaction tx = graphDb.beginTx()) {
			Node n1 = tx.createNode();
			Node n2 = tx.createNode(L2);
			Node n3 = tx.createNode(L2, L3);

			Set<String> expected = new HashSet<>();
			Relationship n1n1T1 = n1.createRelationshipTo(n1, T1);
			n1n1T1.setProperty("name", "n1n1T1");
			expected.add("n1n1T1");

			Relationship n1n2T1_1 = n1.createRelationshipTo(n2, T1);
			n1n2T1_1.setProperty("name", "n1n2T1_1");
			expected.add("n1n2T1_1");

			Relationship n1n2T1_2 = n1.createRelationshipTo(n2, T1);
			n1n2T1_2.setProperty("name", "n1n2T1_2");
			expected.add("n1n2T1_2");

			Relationship n1n2T2 = n1.createRelationshipTo(n1, T2);
			n1n2T2.setProperty("name", "n1n2T2");
			expected.add("n1n2T2");

			Relationship n2n3T3 = n2.createRelationshipTo(n3, T3);
			n2n3T3.setProperty("name", "n2n3T3");
			expected.add("n2n3T3");

			Set<String> actual = new HashSet<>();
			tx.getAllRelationships().forEach(rel -> actual.add((String) rel.getProperty("name")));
			assertEquals(expected, actual);
		}
	}

	@Test
	void testAcquireWriteLock() {
		fail("Not yet implemented");
	}

	@Test
	void testAcquireReadLock() {
		fail("Not yet implemented");
	}

	@Test
	void testSchema() {
		fail("Not yet implemented");
	}

}
