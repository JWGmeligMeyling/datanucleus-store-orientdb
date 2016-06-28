package org.datanucleus.store.orient.fieldmanager;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.orient.OrientUtils;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jan-Willem Gmelig Meyling
 */
public class StoreFieldManager extends AbstractStoreFieldManager {

    protected final Table table;
    protected final OrientElement orientElement;

    public StoreFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, boolean insert, Table table, OrientElement orientElement) {
        super(ec, cmd, insert);
        this.table = table;
        this.orientElement = orientElement;
    }

    public StoreFieldManager(ObjectProvider op, boolean insert, Table table, OrientElement orientElement) {
        super(op, insert);
        this.table = table;
        this.orientElement = orientElement;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeBooleanField(int, boolean)
     */
    @Override
    public void storeBooleanField(int fieldNumber, boolean value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        orientElement.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeByteField(int, byte)
     */
    @Override
    public void storeByteField(int fieldNumber, byte value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        orientElement.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeCharField(int, char)
     */
    @Override
    public void storeCharField(int fieldNumber, char value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        orientElement.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeDoubleField(int, double)
     */
    @Override
    public void storeDoubleField(int fieldNumber, double value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        orientElement.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeFloatField(int, float)
     */
    @Override
    public void storeFloatField(int fieldNumber, float value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        orientElement.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeIntField(int, int)
     */
    @Override
    public void storeIntField(int fieldNumber, int value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        orientElement.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeLongField(int, long)
     */
    @Override
    public void storeLongField(int fieldNumber, long value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        orientElement.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeShortField(int, short)
     */
    @Override
    public void storeShortField(int fieldNumber, short value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        orientElement.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeStringField(int, java.lang.String)
     */
    @Override
    public void storeStringField(int fieldNumber, String value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String propName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (value == null)
        {
            if (!insert)
            {
                orientElement.removeProperty(propName);
            }
            return;
        }
        orientElement.setProperty(propName, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeObjectField(int, java.lang.Object)
     */
    @Override
    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (!isStorable(mmd))
        {
            return;
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded Field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Embedded PC object
                if ((insert && !mmd.isCascadePersist()) || (!insert && !mmd.isCascadeUpdate()))
                {
                    if (!ec.getApiAdapter().isDetached(value) && !ec.getApiAdapter().isPersistent(value))
                    {
                        // Related PC object not persistent, but cant do cascade-persist so throw exception
                        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                        {
                            NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                        }
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), value);
                    }
                }

                // TODO Cater for nulled embedded object on update

                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                embMmds.add(mmd);

                if (value == null)
                {
                    AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    int[] embMmdPosns = embCmd.getAllMemberPositions();
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(ec, embCmd, insert, table, orientElement, embMmds);
                    for (int i=0;i<embMmdPosns.length;i++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embMmdPosns[i]);
                        if (String.class.isAssignableFrom(embMmd.getType()) || embMmd.getType().isPrimitive() || ClassUtils.isPrimitiveWrapperType(mmd.getTypeName()))
                        {
                            // Remove property for any primitive/wrapper/String fields
                            List<AbstractMemberMetaData> colEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                            colEmbMmds.add(embMmd);
                            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(colEmbMmds);
                            orientElement.removeProperty(mapping.getColumn(0).getName());
                        }
                        else if (Object.class.isAssignableFrom(embMmd.getType()))
                        {
                            storeEmbFM.storeObjectField(embMmdPosns[i], null);
                        }
                    }
                    return;
                }

                AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(value.getClass(), clr);
                if (embcmd == null)
                {
                    throw new NucleusUserException("Field " + mmd.getFullFieldName() +
                        " specified as embedded but metadata not found for the class of type " + mmd.getTypeName());
                }

                ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, op, mmd);
                // TODO Cater for inherited embedded objects (discriminator)

                FieldManager ffm = new StoreEmbeddedFieldManager(embOP, insert, table, orientElement, embMmds);
                embOP.provideFields(embcmd.getAllMemberPositions(), ffm);
                return;
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // TODO Support embedded collections, arrays, maps?
                throw new NucleusUserException("Don't currently support embedded field : " + mmd.getFullFieldName());
            }
        }

        storeNonEmbeddedObjectField(mmd,relationType, clr, value);
    }

    protected void storeNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr, Object value)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        ExecutionContext ec = op.getExecutionContext();
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        if (value == null)
        {
            if (insert)
            {
                // Don't store the property when null
            }
            else
            {
                // Has been set to null so remove the property
                for (int i=0;i<mapping.getNumberOfColumns();i++)
                {
                    String colName = mapping.getColumn(i).getName();
                    if (orientElement.getPropertyKeys().contains(colName))
                    {
                        orientElement.removeProperty(colName);
                    }
                }
            }
            return;
        }

        if (mmd.isSerialized())
        {
            if (value instanceof Serializable)
            {
                TypeConverter<Serializable, String> conv = ec.getTypeManager().getTypeConverterForType(Serializable.class, String.class);
                String strValue = conv.toDatastoreType((Serializable) value);
                orientElement.setProperty(mapping.getColumn(0).getName(), strValue);
                return;
            }

            throw new NucleusUserException("Field " + mmd.getFullFieldName() + " is marked as serialised, but value is not Serializable");
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            if (!(orientElement instanceof OrientVertex))
            {
                // TODO Work out if this is the source or the target
                throw new NucleusUserException("Object " + op + " is mapped to a Relationship. Not yet supported");
            }

            OrientVertex node = (OrientVertex) orientElement;
            processSingleValuedRelationForNode(mmd, relationType, value, ec, clr, node);
            return;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            if (!(orientElement instanceof OrientVertex))
            {
                // Any object mapped as a Relationship cannot have multi-value relations, only a source and target
                throw new NucleusUserException("Object " + op + " is mapped to a Relationship but has field " +
                    mmd.getFullFieldName() + " which is multi-valued. This is illegal");
            }

            OrientVertex node = (OrientVertex) orientElement;
//            processMultiValuedRelationForNode(mmd, relationType, value, ec, clr, node);
        }
        else
        {
            if (mapping.getTypeConverter() != null)
            {
                // Persist using the provided converter
                Object datastoreValue = mapping.getTypeConverter().toDatastoreType(value);
                if (mapping.getNumberOfColumns() > 1)
                {
                    for (int i=0;i<mapping.getNumberOfColumns();i++)
                    {
                        // TODO Persist as the correct column type since the typeConverter type may not be directly persistable
                        Object colValue = Array.get(datastoreValue, i);
                        orientElement.setProperty(mapping.getColumn(i).getName(), colValue);
                    }
                }
                else
                {
                    orientElement.setProperty(mapping.getColumn(0).getName(), datastoreValue);
                }
            }
            else
            {
                Object storedValue = OrientUtils.getStoredValueForField(ec, mmd, value, FieldRole.ROLE_FIELD);
                if (storedValue != null)
                {
                    // Neo4j doesn't allow null values
                    orientElement.setProperty(mapping.getColumn(0).getName(), storedValue);
                }
            }
        }

        SCOUtils.wrapSCOField(op, fieldNumber, value, true);
    }

    protected void processSingleValuedRelationForNode(AbstractMemberMetaData mmd, RelationType relationType, Object value,
                                                      ExecutionContext ec, ClassLoaderResolver clr, OrientVertex node)
    {
        if ((insert && !mmd.isCascadePersist()) || (!insert && !mmd.isCascadeUpdate()))
        {
            if (!ec.getApiAdapter().isDetached(value) && !ec.getApiAdapter().isPersistent(value))
            {
                // Related PC object not persistent, but cant do cascade-persist so throw exception
                if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                {
                    NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                }
                throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), value);
            }
        }

//        // 1-1/N-1 Make sure it is persisted and form the relation
//        Object valuePC = (value != null ? ec.persistObjectInternal(value, null, -1, -1) : null);
//        ObjectProvider relatedOP = (value != null ? ec.findObjectProvider(valuePC) : null);
//
//        if (relationType != RelationType.MANY_TO_ONE_BI && mmd.getMappedBy() == null)
//        {
//            // Only have a Relationship if this side owns the relation
//            OrientVertex relatedNode = (OrientVertex)
//                (value != null ? OrientUtils.getPropertyContainerForObjectProvider(orientElement.getGraphDatabase(), relatedOP) : null);
//
//            boolean hasRelation = false;
//            if (!insert)
//            {
//                // Check for old value and remove Relationship if to a different Node
//                Iterable<Relationship> rels = node.getRelationships(DNRelationshipType.SINGLE_VALUED);
//                Iterator<Relationship> relIter = rels.iterator();
//                while (relIter.hasNext())
//                {
//                    Relationship rel = relIter.next();
//                    if (rel.getProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME).equals(mmd.getName()))
//                    {
//                        // Check if existing relationship for this field is to the same node
//                        Node currentNode = rel.getOtherNode(node);
//                        if (currentNode.equals(relatedNode))
//                        {
//                            hasRelation = true;
//                            break;
//                        }
//
//                        // Remove old Relationship TODO Cascade delete?
//                        rel.delete();
//                    }
//                }
//            }
//
//            if (!hasRelation && relatedNode != null)
//            {
//                // Add the new Relationship
//                Relationship rel = node.createRelationshipTo(relatedNode, DNRelationshipType.SINGLE_VALUED);
//                rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME, mmd.getName());
//                if (RelationType.isBidirectional(relationType))
//                {
//                    AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
//                    rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER, relMmds[0].getName());
//                }
//            }
//        }
    }

    /**
     * Convenience method that finds all relationships from the provided owner node and deletes all that
     * are for the specified field.
     * @param ownerNode The owner Node
     * @param mmd Metadata for the member that we are removing relationships for
     */
    private void deleteRelationshipsForMultivaluedMember(OrientVertex ownerNode, AbstractMemberMetaData mmd)
    {
        for (Edge edge : ownerNode.getEdges(Direction.OUT, mmd.getName()))
            edge.remove();
    }

}
