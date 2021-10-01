package com.masterqiao.dasiy;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileUtils;

public class DatabaseConfig {
	private static final Path databaseDirectory = Path.of("database");

	private static void registerShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (DatabaseConfig.driver != null) {
					DatabaseConfig.driver.close();
				}
				if (DatabaseConfig.dbms != null) {
					DatabaseConfig.dbms.shutdown();
				}
			}
		});
	}

	private static DatabaseManagementService dbms = null;
	private static Driver driver = null;
	private static GraphDatabaseService graphDatabaseService = null;

	private static final boolean useLocalDatabase = true;
	private static final boolean useNativeApi = true;

	public static GraphDatabaseService getGraphDatabaseService() throws IOException {
		if (graphDatabaseService != null) {
			return graphDatabaseService;
		}

		if (useLocalDatabase) {
			if (dbms == null) {
				FileUtils.deleteDirectory(databaseDirectory);
				dbms = new DatabaseManagementServiceBuilder(databaseDirectory).setConfig(BoltConnector.enabled, true)
						.setConfig(BoltConnector.listen_address, new SocketAddress("localhost", 7677)).build();
			}
			if (useNativeApi) {
				graphDatabaseService = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
			} else {
				driver = GraphDatabase.driver("bolt://localhost:7677");
				graphDatabaseService = new RemoteGraphDatabaseService(driver);
			}
		} else {
			String username = "neo4j";
			String password = "xxxxxx";
			driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic(username, password));
			graphDatabaseService = new RemoteGraphDatabaseService(driver);
		}

		registerShutdownHook();
		return graphDatabaseService;
	}

}
