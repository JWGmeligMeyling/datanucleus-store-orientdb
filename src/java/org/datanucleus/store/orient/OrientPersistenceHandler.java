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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.orient.fieldmanager.FetchFieldManager;
import org.datanucleus.store.orient.fieldmanager.StoreFieldManager;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import java.util.Iterator;

/**
 * Persistence handler for persisting to Orient datastores.
 */
public class OrientPersistenceHandler extends AbstractPersistenceHandler
{
    /** Localiser for messages. */
    {
        Localiser.registerBundle("org.datanucleus.store.orient.Localisation",
            OrientStoreManager.class.getClassLoader());
    }

    public OrientPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#close()
     */
    public void close()
    {
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractPersistenceHandler#insertObjects(org.datanucleus.store.ObjectProvider[])
     */
    @Override
    public void insertObjects(ObjectProvider... ops)
    {
        ExecutionContext ec = ops[0].getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            OrientGraph db = (OrientGraph)mconn.getConnection();

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.InsertObjects.Start", StringUtils.objectArrayToString(ops)));
            }

            // Do initial insert to create PropertyContainers (Node/Relationship)
            for (ObjectProvider op : ops)
            {
                insertObjectToPropertyContainer(op, db);
            }

            // Do second pass for relation fields
            for (ObjectProvider op : ops)
            {
                AbstractClassMetaData cmd = op.getClassMetaData();
                Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
                OrientElement propObj = (OrientElement)op.getAssociatedValue(OrientStoreManager.OBJECT_PROVIDER_PROPCONTAINER);

                // Process relation fields
                int[] relPositions = cmd.getRelationMemberPositions(ec.getClassLoaderResolver(), ec.getMetaDataManager());
                if (relPositions.length > 0)
                {
                    StoreFieldManager fm = new StoreFieldManager(op, true, table, propObj);
                    op.provideFields(relPositions, fm);
                }
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception inserting objects ", e);
            throw new NucleusDataStoreException("Exception inserting objects", e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method that checks for existence of a PropertyContainer for the specified ObjectProvider,
     * and creates it when not existing, setting all properties except for any relation fields.
     * @param op ObjectProvider
     * @param db The GraphDB
     * @return The PropertyContainer
     * @throws NucleusUserException if a property container exists already with this identity or if an error
     *     occurred during persistence.
     */
    public OrientElement insertObjectToPropertyContainer(ObjectProvider op, OrientGraph db)
    {
        assertReadOnlyForUpdateOfObject(op);

        AbstractClassMetaData cmd = op.getClassMetaData();
        if ((cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE) &&
            !cmd.pkIsDatastoreAttributed(storeMgr))
        {
            // Enforce uniqueness of datastore rows
            try
            {
                locateObject(op);
                throw new NucleusUserException(Localiser.msg("Neo4j.Insert.ObjectWithIdAlreadyExists", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }
            catch (NucleusObjectNotFoundException onfe)
            {
                // Do nothing since object with this id doesn't exist
            }
        }

        ExecutionContext ec = op.getExecutionContext();
        if (!storeMgr.managesClass(cmd.getFullClassName()))
        {
            storeMgr.manageClasses(ec.getClassLoaderResolver(), cmd.getFullClassName());
        }
        Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();

        // Create the PropertyContainer; currently only support as a Node.
        // TODO Support persisting as "attributed relation" where the object has source and target objects and no other relation field
        OrientElement propObj = db.addVertex("class:" + op.getClassMetaData().getName());
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_NATIVE.debug("Persisting " + op + " as " + propObj);
        }
//        addPropertyContainerToTypeIndex(db, propObj, cmd, false);

        // Cache the PropertyContainer with the ObjectProvider
        op.setAssociatedValue(OrientStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);

        if (cmd.pkIsDatastoreAttributed(storeMgr))
        {
            Object id = propObj.getId();

            // Set the identity of the object based on the datastore-generated IDENTITY strategy value
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                op.setPostStoreNewObjectId(id);
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.Insert.ObjectPersistedWithIdentity", op.getObjectAsPrintable(), id));
                }
            }
            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                int[] pkFieldNumbers = cmd.getPKMemberPositions();
                for (int i=0;i<pkFieldNumbers.length;i++)
                {
                    AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);
                    if (storeMgr.isStrategyDatastoreAttributed(cmd, pkFieldNumbers[i]))
                    {
                        if (!Number.class.isAssignableFrom(mmd.getType()) && mmd.getType() != long.class && mmd.getType() != int.class)
                        {
                            // Field type must be Long since Neo4j node id is a long
                            throw new NucleusUserException("Any field using IDENTITY value generation with Neo4j should be of type numeric");
                        }
                        op.setPostStoreNewObjectId(id);
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.Insert.ObjectPersistedWithIdentity", op.getObjectAsPrintable(), id));
                        }
                    }
                }
            }
        }

        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            String propName = table.getDatastoreIdColumn().getName();
            Object key = IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId());
            propObj.setProperty(propName, key);
        }

        if (cmd.isVersioned())
        {
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            String propName = table.getVersionColumn().getName();
            if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
            {
                long versionNumber = 1;
                op.setTransactionalVersion(Long.valueOf(versionNumber));
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(Localiser.msg("Neo4j.Insert.ObjectPersistedWithVersion",
                        op.getObjectAsPrintable(), op.getInternalObjectId(), "" + versionNumber));
                }
                if (vermd.getFieldName() != null)
                {
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    Object verFieldValue = Long.valueOf(versionNumber);
                    if (verMmd.getType() == int.class || verMmd.getType() == Integer.class)
                    {
                        verFieldValue = Integer.valueOf((int)versionNumber);
                    }
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), verFieldValue);
                }
                else
                {
                    propObj.setProperty(propName, versionNumber);
                }
            }
        }

        if (cmd.hasDiscriminatorStrategy())
        {
            // Add discriminator field
            DiscriminatorMetaData discmd = cmd.getDiscriminatorMetaData();
            String propName = table.getDiscriminatorColumn().getName();
            Object discVal = null;
            if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
            {
                discVal = cmd.getFullClassName();
            }
            else
            {
                discVal = discmd.getValue();
            }
            propObj.setProperty(propName, discVal);
        }

        // Add multi-tenancy discriminator if applicable
//        if (ec.getNucleusContext().isClassMultiTenant(cmd))
//        {
//            propObj.setProperty(table.getMultitenancyColumn().getName(), ec.getNucleusContext().getMultiTenancyId(ec, cmd));
//        }

        // Insert non-relation fields
        int[] nonRelPositions = cmd.getNonRelationMemberPositions(ec.getClassLoaderResolver(), ec.getMetaDataManager());
        StoreFieldManager fm = new StoreFieldManager(op, true, table, propObj);
        op.provideFields(nonRelPositions, fm);

        return propObj;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#insertObject(org.datanucleus.store.ObjectProvider)
     */
    public void insertObject(ObjectProvider op)
    {
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            OrientGraph db = (OrientGraph) mconn.getConnection();

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.Insert.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            // Step 1 : Create PropertyContainer with non-relation fields
            OrientElement propObj = insertObjectToPropertyContainer(op, db);
            AbstractClassMetaData cmd = op.getClassMetaData();
            Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();

            // Step 2 : Set relation fields
            int[] relPositions = cmd.getRelationMemberPositions(ec.getClassLoaderResolver(), ec.getMetaDataManager());
            if (relPositions.length > 0)
            {
                StoreFieldManager fm = new StoreFieldManager(op, true, table, propObj);
                op.provideFields(relPositions, fm);
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception inserting object " + op, e);
            throw new NucleusDataStoreException("Exception inserting object for " + op, e);
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#updateObject(org.datanucleus.store.ObjectProvider, int[])
     */
    public void updateObject(ObjectProvider op, int[] fieldNumbers)
    {
        assertReadOnlyForUpdateOfObject(op);

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            OrientGraph db = (OrientGraph) mconn.getConnection();

            long startTime = System.currentTimeMillis();
            AbstractClassMetaData cmd = op.getClassMetaData();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                StringBuilder fieldStr = new StringBuilder();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.Update.Start", op.getObjectAsPrintable(), op.getInternalObjectId(), fieldStr.toString()));
            }

            if (!storeMgr.managesClass(cmd.getFullClassName()))
            {
                storeMgr.manageClasses(op.getExecutionContext().getClassLoaderResolver(), cmd.getFullClassName());
            }
            Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();

            OrientElement propObj = OrientUtils.getPropertyContainerForObjectProvider(db, op);
            if (propObj == null)
            {
                if (cmd.isVersioned())
                {
                    throw new NucleusOptimisticException("Object with id " + op.getInternalObjectId() +
                        " and version " + op.getTransactionalVersion() + " no longer present");
                }
                throw new NucleusDataStoreException("Could not find object with id " + op.getInternalObjectId());
            }

            int[] updatedFieldNums = fieldNumbers;
            if (cmd.isVersioned())
            {
                // Version object so calculate version to store with
                Object currentVersion = op.getTransactionalVersion();
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                Object nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), currentVersion);
                op.setTransactionalVersion(nextVersion);

                if (vermd.getFieldName() != null)
                {
                    // Update the version field value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);

                    boolean updatingVerField = false;
                    for (int i=0;i<fieldNumbers.length;i++)
                    {
                        if (fieldNumbers[i] == verMmd.getAbsoluteFieldNumber())
                        {
                            updatingVerField = true;
                        }
                    }
                    if (!updatingVerField)
                    {
                        // Add the version field to the fields to be updated
                        updatedFieldNums = new int[fieldNumbers.length+1];
                        System.arraycopy(fieldNumbers, 0, updatedFieldNums, 0, fieldNumbers.length);
                        updatedFieldNums[fieldNumbers.length] = verMmd.getAbsoluteFieldNumber();
                    }
                }
                else
                {
                    // Update the stored surrogate value
                    String propName = table.getVersionColumn().getName();
                    propObj.setProperty(propName, nextVersion);
                }
            }

            StoreFieldManager fm = new StoreFieldManager(op, false, table, propObj);
            op.provideFields(updatedFieldNums, fm);

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug("Updating " + op + " in " + propObj);
            }

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.ExecutionTime",
                    (System.currentTimeMillis() - startTime)));
            }
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception updating object " + op, e);
            throw new NucleusDataStoreException("Exception updating object for " + op, e);
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#deleteObject(org.datanucleus.store.ObjectProvider)
     */
    public void deleteObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        AbstractClassMetaData cmd = op.getClassMetaData();
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            OrientGraph db = (OrientGraph)mconn.getConnection();

            OrientElement propObj = OrientUtils.getPropertyContainerForObjectProvider(db, op);
            if (propObj == null)
            {
                throw new NucleusException("Attempt to delete " + op + " yet no Node/Relationship found! See the log for details");
            }

            // Invoke any cascade deletion
            op.loadUnloadedFields();
            int[] relMemberPosns = cmd.getRelationMemberPositions(ec.getClassLoaderResolver(), ec.getMetaDataManager());
            op.provideFields(relMemberPosns, new DeleteFieldManager(op, true));

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.Delete.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            if (propObj instanceof OrientVertex)
            {
                // Remove all Relationships for this Node
                OrientVertex node = (OrientVertex)propObj;
                Iterable<Edge> rels = node.getEdges(Direction.BOTH);
                Iterator<Edge> relIter = rels.iterator();
                while (relIter.hasNext())
                {
                    Edge rel = relIter.next();
                    if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_NATIVE.debug("Deleting relationship " + rel + " for " + node);
                    }
                    rel.remove();
                }

                // Remove it from the DN_TYPES index
//                db.index().forNodes(OrientStoreManager.PROPCONTAINER_TYPE_INDEX).remove(node);

                // Delete this object
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug("Deleting " + op + " as " + node);
                }
                node.remove();
            }
            else
            {
                // TODO Cater for persistence as Relationship
            }

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception deleting object " + op, e);
            throw new NucleusDataStoreException("Exception deleting object for " + op, e);
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#fetchObject(org.datanucleus.store.ObjectProvider, int[])
     */
    public void fetchObject(ObjectProvider op, int[] fieldNumbers)
    {
        AbstractClassMetaData cmd = op.getClassMetaData();

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            OrientGraph db = (OrientGraph) mconn.getConnection();

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                // Debug information about what we are retrieving
                StringBuilder str = new StringBuilder("Fetching object \"");
                str.append(op.getObjectAsPrintable()).append("\" (id=");
                str.append(op.getInternalObjectId()).append(")").append(" fields [");
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        str.append(",");
                    }
                    str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                str.append("]");
                NucleusLogger.DATASTORE_RETRIEVE.debug(str.toString());
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("Neo4j.Fetch.Start", op.getObjectAsPrintable(), op.getInternalObjectId()));
            }

            if (!storeMgr.managesClass(cmd.getFullClassName()))
            {
                storeMgr.manageClasses(op.getExecutionContext().getClassLoaderResolver(), cmd.getFullClassName());
            }
            Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();

            // Find the Node for this ObjectProvider
            OrientElement propObj = OrientUtils.getPropertyContainerForObjectProvider(db, op);
            if (propObj == null)
            {
                throw new NucleusObjectNotFoundException("Datastore object for " + op + " is not found");
            }

            // Retrieve the fields required
            FieldManager fm = new FetchFieldManager(op, propObj, table);
            op.replaceFields(fieldNumbers, fm);

            if (cmd.isVersioned() && op.getTransactionalVersion() == null)
            {
                // No version set, so retrieve it
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Version stored in a field
                    Object datastoreVersion = op.provideField(cmd.getAbsolutePositionOfMember(vermd.getFieldName()));
                    op.setVersion(datastoreVersion);
                }
                else
                {
                    // Surrogate version
                    String propName = table.getVersionColumn().getName();
                    Object datastoreVersion = propObj.getProperty(propName);
                    op.setVersion(datastoreVersion);
                }
            }

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("Neo4j.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementFetchCount();
            }
        }
        catch (Exception e)
        {
            NucleusLogger.DATASTORE_RETRIEVE.error("Exception on fetch of fields", e);
            throw new NucleusDataStoreException("Exception on fetch of fields", e);
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#locateObject(org.datanucleus.store.ObjectProvider)
     */
    public void locateObject(ObjectProvider op)
    {
        final AbstractClassMetaData cmd = op.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION ||
            cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            ExecutionContext ec = op.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnection(ec);
            try
            {
                if (!storeMgr.managesClass(cmd.getFullClassName()))
                {
                    storeMgr.manageClasses(ec.getClassLoaderResolver(), cmd.getFullClassName());
                }

                OrientGraph db = (OrientGraph) mconn.getConnection();
                OrientElement propObj = OrientUtils.getPropertyContainerForObjectProvider(db, op);
                if (propObj == null)
                {
                    throw new NucleusObjectNotFoundException("Object not found for id=" + op.getInternalObjectId());
                }
            }
            finally
            {
                mconn.release();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#findObject(org.datanucleus.store.ExecutionContext, java.lang.Object)
     */
    public Object findObject(ExecutionContext ec, Object id)
    {
        ManagedConnection mconn = storeMgr.getConnection(ec);
        OrientGraph db = (OrientGraph) mconn.getConnection();
        OrientElement element = db.getElement(id);
        return null;
    }
}
