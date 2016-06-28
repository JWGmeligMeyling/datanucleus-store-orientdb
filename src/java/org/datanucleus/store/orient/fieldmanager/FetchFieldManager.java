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
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.orient.OrientUtils;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.MultiColumnConverter;
import org.datanucleus.store.types.converters.TypeConverter;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jan-Willem Gmelig Meyling
 */
public class FetchFieldManager extends AbstractFetchFieldManager {

    protected final Table table;

    protected final OrientElement orientElement;

    boolean embedded = false;

    public FetchFieldManager(ObjectProvider op, OrientElement orientElement, Table table) {
        super(op);
        this.orientElement = orientElement;
        this.table = table;
    }

    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, OrientElement orientElement, Table table) {
        super(ec, cmd);
        this.orientElement = orientElement;
        this.table = table;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber) {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    @Override
    public boolean fetchBooleanField(int fieldNumber) {
        return (Boolean) orientElement.getProperty(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    @Override
    public char fetchCharField(int fieldNumber) {
        return (Character) orientElement.getProperty(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    @Override
    public byte fetchByteField(int fieldNumber) {
        return (Byte) orientElement.getProperty(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    @Override
    public short fetchShortField(int fieldNumber) {
        return (Short) orientElement.getProperty(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    @Override
    public int fetchIntField(int fieldNumber) {
        return (Integer) orientElement.getProperty(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    @Override
    public long fetchLongField(int fieldNumber) {
        return (Long) orientElement.getProperty(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    @Override
    public float fetchFloatField(int fieldNumber) {
        return (Float) orientElement.getProperty(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    @Override
    public double fetchDoubleField(int fieldNumber) {
        return (Double) orientElement.getProperty(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    @Override
    public String fetchStringField(int fieldNumber) {
        return (String) orientElement.getProperty(getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    @Override
    public Object fetchObjectField(int fieldNumber) {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
        {
            return op.provideField(fieldNumber);
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE)
        {
            if (MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
            {
                // Embedded field
                if (RelationType.isRelationSingleValued(relationType))
                {
                    // Embedded PC object
                    AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    if (embcmd == null)
                    {
                        throw new NucleusUserException("Field " + mmd.getFullFieldName() + " marked as embedded but no such metadata");
                    }

                    // TODO Cater for null (use embmd.getNullIndicatorColumn/Value)

                    // TODO Cater for inherited embedded objects (discriminator)

                    List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                    embMmds.add(mmd);
                    ObjectProvider embOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, embcmd, op, fieldNumber);
                    FieldManager ffm = new FetchFieldManager(
                        embOP,
//                      embMmds,
                        orientElement,
                        table
                    );
                    embOP.replaceFields(embcmd.getAllMemberPositions(), ffm);
                    return embOP.getObject();
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    throw new NucleusUserException("Dont currently support embedded multivalued field : " + mmd.getFullFieldName());
                }
            }
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }

    protected Object fetchNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        if (RelationType.isRelationSingleValued(relationType))
        {
            if (!(orientElement instanceof OrientVertex))
            {
                throw new NucleusUserException("Object " + op + " is mapped to a Relationship. Not yet supported");
            }

            OrientVertex node = (OrientVertex)orientElement;
            return processSingleValuedRelationForNode(mmd, relationType, ec, clr, node);
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            if (!(orientElement instanceof OrientVertex))
            {
                // Any object mapped as a Relationship cannot have multi-value relations, only a source and target
                throw new NucleusUserException("Object " + op + " is mapped to a Relationship but has field " +
                    mmd.getFullFieldName() + " which is multi-valued. This is illegal");
            }

            OrientVertex node = (OrientVertex)orientElement;
            return null;
//            return processMultiValuedRelationForNode(mmd, relationType, ec, clr, node);
        }

        MemberColumnMapping mapping = getColumnMapping(fieldNumber);
        String propName = mapping.getColumn(0).getName(); // TODO Support multicol members
        if (!orientElement.getPropertyKeys().contains(propName))
        {
            return null;
        }
        Object value = orientElement.getProperty(propName);

        if (mmd.isSerialized())
        {
            if (value instanceof String)
            {
                TypeConverter<Serializable, String> conv = ec.getTypeManager().getTypeConverterForType(Serializable.class, String.class);
                return conv.toMemberType((String) value);
            }

            throw new NucleusUserException("Field " + mmd.getFullFieldName() + " has a serialised value," +
                " but we only support String serialisation and is " + value.getClass().getName());
        }

        Object returnValue = null;
        if (mapping.getTypeConverter() != null)
        {
            TypeConverter conv = mapping.getTypeConverter();
            if (mapping.getNumberOfColumns() > 1)
            {
                boolean isNull = true;
                Object valuesArr = null;
                Class[] colTypes = ((MultiColumnConverter)conv).getDatastoreColumnTypes();
                if (colTypes[0] == int.class)
                {
                    valuesArr = new int[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == long.class)
                {
                    valuesArr = new long[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == double.class)
                {
                    valuesArr = new double[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == float.class)
                {
                    valuesArr = new double[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == String.class)
                {
                    valuesArr = new String[mapping.getNumberOfColumns()];
                }
                // TODO Support other types
                else
                {
                    valuesArr = new Object[mapping.getNumberOfColumns()];
                }

                for (int i=0;i<mapping.getNumberOfColumns();i++)
                {
                    String colName = mapping.getColumn(i).getName();
                    if (orientElement.getPropertyKeys().contains(colName))
                    {
                        isNull = false;
                        Array.set(valuesArr, i, orientElement.getProperty(colName));
                    }
                    else
                    {
                        Array.set(valuesArr, i, null);
                    }
                }

                if (isNull)
                {
                    return null;
                }

                Object memberValue = conv.toMemberType(valuesArr);
                if (op != null && memberValue != null)
                {
                    memberValue = SCOUtils.wrapSCOField(op, fieldNumber, memberValue, true);
                }
                return memberValue;
            }

            String colName = mapping.getColumn(0).getName();
            if (!orientElement.getPropertyKeys().contains(colName))
            {
                return null;
            }
            Object propVal = orientElement.getProperty(colName);
            returnValue = conv.toMemberType(propVal);

            if (op != null)
            {
                returnValue = SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), returnValue, true);
            }
            return returnValue;
        }

        Object fieldValue = OrientUtils.getFieldValueFromStored(ec, mmd, value, FieldRole.ROLE_FIELD);
        if (op != null)
        {
            // Wrap if SCO
            return SCOUtils.wrapSCOField(op, mmd.getAbsoluteFieldNumber(), fieldValue, true);
        }
        return fieldValue;
    }


    protected Object processSingleValuedRelationForNode(AbstractMemberMetaData mmd, RelationType relationType,
                                                        ExecutionContext ec, ClassLoaderResolver clr, OrientVertex node)
    {

        AbstractClassMetaData relCmd = null;
        String propNameValue = mmd.getName();
        if (mmd.getMappedBy() != null)
        {
            AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
            relCmd = relMmds[0].getAbstractClassMetaData();
        }
        else if (relationType == RelationType.MANY_TO_ONE_BI)
        {
            AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
            relCmd = relMmds[0].getAbstractClassMetaData();
        }
        else
        {
            relCmd = ec.getMetaDataManager().getMetaDataForClass(propNameValue, clr);
        }

        for (Edge edge : node.getEdges(Direction.OUT, mmd.getName())) {
            OrientVertex relNode = (OrientVertex) edge.getVertex(Direction.OUT);
            return OrientUtils.getObjectForPropertyContainer(relNode,
                OrientUtils.getClassMetaDataForPropertyContainer(relNode, ec, relCmd), ec, false);
        }

        for (Edge edge : node.getEdges(Direction.IN, mmd.getName())) {
            OrientVertex relNode = (OrientVertex) edge.getVertex(Direction.IN);
            return OrientUtils.getObjectForPropertyContainer(relNode,
                OrientUtils.getClassMetaDataForPropertyContainer(relNode, ec, relCmd), ec, false);
        }

        return null;
    }

}
