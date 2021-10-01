package com.masterqiao.dasiy;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Driver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;

public class RemoteGraphDatabaseService implements GraphDatabaseService {

	private Driver driver;

	public RemoteGraphDatabaseService(Driver driver) {
		this.driver = driver;
	}

	@Override
	public boolean isAvailable(long timeout) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Transaction beginTx() {
		return new RemoteTransaction(driver.session());
	}

	@Override
	public Transaction beginTx(long timeout, TimeUnit unit) {
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public void executeTransactionally(String query) throws QueryExecutionException {
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public void executeTransactionally(String query, Map<String, Object> parameters) throws QueryExecutionException {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public <T> T executeTransactionally(String query, Map<String, Object> parameters,
			ResultTransformer<T> resultTransformer) throws QueryExecutionException {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public <T> T executeTransactionally(String query, Map<String, Object> parameters,
			ResultTransformer<T> resultTransformer, Duration timeout) throws QueryExecutionException {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public String databaseName() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet");
	}

}
