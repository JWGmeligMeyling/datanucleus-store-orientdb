/**********************************************************************
Copyright (c) 2010 Luigi Dell'Aquila and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
 **********************************************************************/
package org.datanucleus.store.orient;

import java.util.Map;

import javax.transaction.xa.XAResource;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.datanucleus.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;

/**
 * Implementation of a ConnectionFactory for Orient Database. </p>
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory {
	{
		Localiser.registerBundle("org.datanucleus.store.orient.Localisation",
			OrientStoreManager.class.getClassLoader());
	}

	protected final OrientGraphFactory orientGraphFactory;

	/**
	 * Constructor
	 * 
	 * @param storeManager
	 *          The Store Manager
	 * @param resourceType
	 *          Type of resource (tx, nontx)
	 */
	public ConnectionFactoryImpl(StoreManager storeManager, String resourceType) {
		super(storeManager, resourceType);

		this.orientGraphFactory = new OrientGraphFactory(
			storeManager.getConnectionURL().substring(7),
			storeManager.getConnectionUserName(),
			storeManager.getConnectionPassword()
		);
//		if (!(url.startsWith("remote:") || url.startsWith("local:"))) {
//			throw new NucleusException(LOCALISER_ORIENT.msg("Orient.URLInvalid", url));
//		}
	}

	/**
	 * Obtain a connection from the Factory. The connection will be enlisted within the {@link org.datanucleus.Transaction} associated
	 * to the <code>poolKey</code> if "enlist" is set to true.
	 * 
	 * @param executionContext
	 *          the  ExecutionContext
	 * @param map
	 *          Any options for then creating the connection
	 * @return the {@link org.datanucleus.store.connection.ManagedConnection}
	 */
	public ManagedConnection createManagedConnection(ExecutionContext executionContext, Map map) {
		return new ManagedConnectionImpl(executionContext, map);
	}

	/**
	 * Implementation of a ManagedConnection for Orient.
	 */
	class ManagedConnectionImpl extends AbstractManagedConnection {
		ExecutionContext	omf;

		/**
		 * Constructor.
		 * 
		 * @param omf
		 * @param transactionOptions
		 *          Any options
		 */
		ManagedConnectionImpl(ExecutionContext omf, @SuppressWarnings("rawtypes") Map transactionOptions) {
			this.omf = omf;
		}

		/**
		 * Obtain a XAResource which can be enlisted in a transaction
		 */
		public XAResource getXAResource() {
			return null;
		}

		public OrientGraph getOrientConnection() {
			return (OrientGraph) conn;
		}

		/**
		 * Create a connection to the resource
		 */
		public Object getConnection() {
			if (conn == null) {
				conn = orientGraphFactory.getTx();
				((OrientStoreManager) storeMgr).registerObjectContainer((OrientGraph) conn);
			}
			return conn;
		}

		/**
		 * Close the connection
		 */
		public void close() {
			for (int i = 0; i < listeners.size(); i++) {
				listeners.get(i).managedConnectionPreClose();
			}

			OrientGraph conn = getOrientConnection();
			if (conn != null) {

				String connStr = conn.toString();
				if (commitOnRelease) {
					if (!conn.isClosed()) {
						conn.commit();
						if (NucleusLogger.CONNECTION.isDebugEnabled()) {
							NucleusLogger.CONNECTION.debug(Localiser.msg("Orient.commitOnClose", connStr));// TODO
						}
					}
				}

				if (!conn.isClosed()) {
					conn.shutdown(true);
					if (NucleusLogger.CONNECTION.isDebugEnabled()) {
						NucleusLogger.CONNECTION.debug(Localiser.msg("Orient.closingConnection", connStr));// TODO
					}
				} else {
					if (NucleusLogger.CONNECTION.isDebugEnabled()) {
						NucleusLogger.CONNECTION.debug(Localiser.msg("Orient.connectionAlreadyClosed", connStr));// TODO
					}
				}

			}

			try {
				for (int i = 0; i < listeners.size(); i++) {
					listeners.get(i).managedConnectionPostClose();
				}
			} finally {
				listeners.clear();
			}
			((OrientStoreManager) storeMgr).deregisterObjectContainer(conn);
			this.conn = null;
		}
	}

}
