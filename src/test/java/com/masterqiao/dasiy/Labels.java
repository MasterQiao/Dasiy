package com.masterqiao.dasiy;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public class Labels {
	public enum NodeLabels implements Label {
		L1, L2, L3, L4;
	}

	public enum LinkTypes implements RelationshipType {
		T1, T2, T3
	}
}
