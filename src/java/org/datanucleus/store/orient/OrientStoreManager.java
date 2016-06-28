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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.id.ORID;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.Configuration;
import org.datanucleus.ExecutionContext.LifecycleListener;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.DefaultCandidateExtent;
import org.datanucleus.store.Extent;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.exceptions.NoExtentException;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Store Manager for Orient Database
 */
public class OrientStoreManager extends AbstractStoreManager
{
    public static final String OBJECT_PROVIDER_PROPCONTAINER = "BLIEP";

    /** Localiser for messages. */
    static {
        Localiser.registerBundle(
            "org.datanucleus.store.orient.Localisation",
            OrientStoreManager.class.getClassLoader()
        );
    }

    /**
     * Collection of the currently active ObjectContainers. Used for providing class mapping information when they are
     * found.
     */
    private Set activeObjectContainers = new HashSet();

    /**
     * Constructor for a new Orient StoreManager. Stores the basic information required for the datastore management.
     * @param clr the ClassLoaderResolver
     * @param nucleusContext The corresponding PersistenceNucleusContext.
     * @param props The properties.
     */
    public OrientStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext nucleusContext, Map<String, Object> props)
    {
        super("orient", clr, nucleusContext, props);

        // Log the manager configuration
        logConfiguration();

        // Handler for persistence process
        persistenceHandler = new OrientPersistenceHandler(this);

        // Make sure transactional connection factory has listener for closing object container
        nucleusContext.addExecutionContextListener(new LifecycleListener()
        {
            public void preClose(ExecutionContext ec)
            {
                //TODO is it the right place...?
                OrientGraph conn = (OrientGraph) getConnection(ec).getConnection();
                if(!conn.isClosed()){
                    conn.shutdown();
                }
            }
            
            
        });
    }

    
    
    /**
     * Release of resources
     */
    public void close()
    {
        super.close();
        activeObjectContainers.clear();
    }

    /**
     * Convenience method to log the configuration of this store manager.
     */
    protected void logConfiguration()
    {
        super.logConfiguration();
        if (NucleusLogger.DATASTORE.isDebugEnabled())
        {

            Configuration conf = this.nucleusContext.getConfiguration();

            String outputFilename = conf.getStringProperty("datanucleus.orient.outputFile");
            if (outputFilename != null)
            {
                NucleusLogger.DATASTORE.debug("Orient Output : " + outputFilename);
            }

            NucleusLogger.DATASTORE.debug("===========================================================");
        }
    }

    // ------------------------------- Class Management -----------------------------------


    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#addClass(java.lang.String, org.datanucleus.ClassLoaderResolver)
     */
    public void addClass(String className, ClassLoaderResolver clr)
    {
        addClasses(new String[] {className}, clr);        
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#addClasses(java.lang.String[], org.datanucleus.ClassLoaderResolver)
     */
    public void addClasses(String[] classNames, ClassLoaderResolver clr)
    {
        if (classNames == null)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = 
            this.nucleusContext.getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData)iter.next();
            if (cmd.getPersistenceModifier() != ClassPersistenceModifier.PERSISTENCE_CAPABLE && 
                    cmd.getPersistenceModifier() != ClassPersistenceModifier.PERSISTENCE_AWARE)
            {
                return;
            }
            StoreData sd = storeDataMgr.get(cmd.getFullClassName());
            if (sd == null)
            {
                registerStoreData(newStoreData(cmd, clr));
            }
        }
    }
    
    /**
     * Method to register some data with the store. This will also register the data with the starter process.
     * @param data The StoreData to add
     */
    protected void registerStoreData(StoreData data)
    {
        // Register the data
        super.registerStoreData(data);

        // Make this class config known to all active ObjectContainers for this store
        if (activeObjectContainers.size() > 0)
        {
            Iterator containerIter = activeObjectContainers.iterator();
            while (containerIter.hasNext())
            {
                OrientGraph cont = (OrientGraph) containerIter.next();
                registerClassInOrient(cont, (AbstractClassMetaData) data.getMetaData());
            }
        }
    }

    public void registerClassInOrient(OrientGraph cont, AbstractClassMetaData metaData) {
        assert cont.getVertexType(metaData.getTable()) != null;
    }

    /**
     * Method to register an ObjectContainer as active on this store. Will load up all known class mapping information
     * into the datastore container.
     * @param cont ObjectContainer
     */
    public void registerObjectContainer(OrientGraph cont)
    {
        if (cont == null)
        {
            return;
        }

        // Register all known classes with the ObjectContainer of this transaction

        Collection storeDataValues = storeDataMgr.getManagedStoreData();
        Iterator iter = storeDataValues.iterator();
        while (iter.hasNext())
        {
            StoreData data = (StoreData) iter.next();
            registerClassInOrient(cont, (AbstractClassMetaData) data.getMetaData());
        }
        activeObjectContainers.add(cont);
    }

    /**
     * Method to deregister an ObjectContainer from this store. ObjectContainers are deregistered when about to be
     * closed and hence not interested in more class mapping information.
     * @param cont ObjectContainer
     */
    public void deregisterObjectContainer(OrientGraph cont)
    {
        if (cont == null)
        {
            return;
        }
        activeObjectContainers.remove(cont);
    }

    // ------------------------------- Utilities -----------------------------------


    /**
     * Convenience method to get the identity for a Persistable object.
     * @param ec execution context
     * @param pc The object
     * @return The identity
     */
//    public Object getObjectIdForObject(ExecutionContext ec, Object pc)
//    {
//        AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(pc.getClass().getName(), ec.getClassLoaderResolver());
//        Object id = null;
//        ObjectProvider sm = ec.findObjectProvider(pc);
//        if (sm != null)
//        {
//            // Object is managed, so return its id
//            return sm.getInternalObjectId();
//        }
//
//        OrientGraph cont = (OrientGraph) getConnection(ec).getConnection();
//        try
//        {
//            if (cmd.getIdentityType() == IdentityType.DATASTORE)
//            {
//
//                ORID orid = cont.getRecordByUserObject(pc, false).getIdentity();
//                if (orid == null)
//                {
//                    return null;
//                }
//                String idKey = ORecordId.generateString(((ORecordId) orid).getClusterId(), ((ORecordId) id).getClusterPosition());
//                return OIDFactory.getInstance(getOMFContext(), idKey);
//            }
//            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
//            {
//                // If the fields are loaded then the id is known
//                return getApiAdapter().getNewApplicationIdentityObjectId(pc, cmd);
//            }
//        }
//        finally
//        {
//        }
//
//        return id;
//    }

    /**
     * Accessor for an Extent for a class.
     * @param ec execution context
     * @param c The class requiring the Extent
     * @param subclasses Whether to include subclasses of 'c'
     * @return The Extent.
     */
    public Extent getExtent(ExecutionContext ec, Class c, boolean subclasses)
    {
        AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(c, ec.getClassLoaderResolver());
        if (!cmd.isRequiresExtent())
        {
            throw new NoExtentException(c.getName());
        }

        return new DefaultCandidateExtent(ec, c, subclasses, cmd);
    }

    /**
     * Accessor for the supported options in string form
     */
    public Collection<String> getSupportedOptions()
    {
        Set<String> set = new HashSet<String>();
        set.add("DatastoreIdentity");
        // set.add("ApplicationIdentity");
        set.add("OptimisticTransaction");
        // set.add("TransactionIsolationLevel.read-committed");
        return set;
    }


    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#manageClasses(org.datanucleus.ClassLoaderResolver, java.lang.String[])
     */
    @Override
    public void manageClasses(ClassLoaderResolver clr, String... classNames)
    {
        if (classNames == null)
        {
            return;
        }

        ManagedConnection mconn = getConnection(-1);
        try
        {
            OrientGraph db = (OrientGraph)mconn.getConnection();

            manageClasses(classNames, clr, db);
        }
        finally
        {
            mconn.release();
        }
    }

    public void manageClasses(String[] classNames, ClassLoaderResolver clr, OrientGraph db)
    {
        if (classNames == null)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Set<String> clsNameSet = new HashSet<String>();
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData)iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE && !cmd.isAbstract() && !cmd.isEmbeddedOnly())
            {
                if (!storeDataMgr.managesClass(cmd.getFullClassName()))
                {
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null)
                    {
                        CompleteClassTable table = new CompleteClassTable(this, cmd, null);
                        sd = newStoreData(cmd, clr);
                        sd.setTable(table);
                        registerStoreData(sd);
                    }

                    clsNameSet.add(cmd.getFullClassName());
                }
            }
        }
    }
}
