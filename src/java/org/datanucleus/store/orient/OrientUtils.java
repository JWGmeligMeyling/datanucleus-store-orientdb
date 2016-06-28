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

import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.fieldmanager.FieldManager;

import org.datanucleus.store.orient.fieldmanager.FetchFieldManager;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.wrappers.backed.Collection;
import org.datanucleus.util.ClassUtils;

import java.lang.reflect.Array;

/**
 * Utilities for Orient (http://www.orientechnologies.com).
 */
public class OrientUtils
{
    public static AbstractClassMetaData getClassMetaDataForPropertyContainer(OrientVertex propObj, ExecutionContext ec, AbstractClassMetaData cmd) {
        return cmd;
    }

    public static Object getObjectForPropertyContainer(OrientElement propObj, AbstractClassMetaData cmd, ExecutionContext ec, boolean ignoreCache) {
        int[] fpMembers = ec.getFetchPlan().getFetchPlanForClass(cmd).getMemberNumbers();

        Object obj = null;
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            obj = getObjectUsingApplicationIdForDBObject(propObj, cmd, ec, ignoreCache, fpMembers);
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            obj = getObjectUsingDatastoreIdForDBObject(propObj, cmd, ec, ignoreCache, fpMembers);
        }
        else
        {
            obj = getObjectUsingNondurableIdForDBObject(propObj, cmd, ec, ignoreCache, fpMembers);
        }
        return obj;

    }

    private static Object getObjectUsingNondurableIdForDBObject(OrientElement propObj, AbstractClassMetaData cmd, ExecutionContext ec, boolean ignoreCache, final int[] fpMembers) {
        SCOID id = new SCOID(cmd.getFullClassName());
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, false, false, type.getName());
        ObjectProvider op = ec.findObjectProvider(pc);
        StoreData sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        if (sd == null)
        {
            ec.getStoreManager().manageClasses(ec.getClassLoaderResolver(), cmd.getFullClassName());
            sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        }
        Table table = sd.getTable();

        if (op.getAssociatedValue(OrientStoreManager.OBJECT_PROVIDER_PROPCONTAINER) == null)
        {
            // The returned ObjectProvider doesn't have this Node/Relationship assigned to it hence must be just created
            // so load the fieldValues from it.
            op.setAssociatedValue(OrientStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);
            final FieldManager fm = new FetchFieldManager(ec, cmd, propObj, table);
            op.loadFieldValues(new FieldValues()
            {
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            });

            if (cmd.isVersioned())
            {
                // Set the version on the retrieved object
                Object version = null;
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Get the version from the field value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    version = op.provideField(verMmd.getAbsoluteFieldNumber());
                }
                else
                {
                    // Get the surrogate version from the datastore
                    version = propObj.getProperty(table.getVersionColumn().getName());
                }
                op.setVersion(version);
            }
        }
        return pc;
    }

    private static Object getObjectUsingDatastoreIdForDBObject(OrientElement propObj, AbstractClassMetaData cmd, ExecutionContext ec, boolean ignoreCache, final int[] fpMembers) {
        StoreData sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        if (sd == null)
        {
            ec.getStoreManager().manageClasses(ec.getClassLoaderResolver(), cmd.getFullClassName());
            sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        }
        Table table = sd.getTable();
        Object idKey = propObj.getId();

        Object id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), idKey);
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, false, false, type.getName());
        ObjectProvider op = ec.findObjectProvider(pc);
        if (op.getAssociatedValue(OrientStoreManager.OBJECT_PROVIDER_PROPCONTAINER) == null)
        {
            // The returned ObjectProvider doesn't have this Node/Relationship assigned to it hence must be just created so load the fieldValues from it.
            op.setAssociatedValue(OrientStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);
            final FieldManager fm = new FetchFieldManager(ec, cmd, propObj, table);
            op.loadFieldValues(new FieldValues()
            {
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            });

            if (cmd.isVersioned())
            {
                // Set the version on the retrieved object
                Object version = null;
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Get the version from the field value
                    version = op.provideField(cmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber());
                }
                else
                {
                    // Get the surrogate version from the datastore
                    version = propObj.getProperty(table.getVersionColumn().getName());
                }
                op.setVersion(version);
            }
        }
        return pc;
    }

    private static Object getObjectUsingApplicationIdForDBObject(OrientElement propObj, AbstractClassMetaData cmd, ExecutionContext ec, boolean ignoreCache, final int[] fpMembers) {
        StoreData sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        if (sd == null)
        {
            ec.getStoreManager().manageClasses(ec.getClassLoaderResolver(), cmd.getFullClassName());
            sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        }
        Table table = sd.getTable();
        final FieldManager fm = new FetchFieldManager(ec, cmd, propObj, table);
        Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, fm);

        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, false, false, type.getName());
        ObjectProvider op = ec.findObjectProvider(pc);

        if (op.getAssociatedValue(OrientStoreManager.OBJECT_PROVIDER_PROPCONTAINER) == null)
        {
            // The returned ObjectProvider doesn't have this Node/Relationship assigned to it hence must be just created
            // so load the fieldValues from it.
            op.setAssociatedValue(OrientStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);
            op.loadFieldValues(new FieldValues()
            {
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            });

            if (cmd.isVersioned())
            {
                // Set the version on the retrieved object
                Object version = null;
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Get the version from the field value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    version = op.provideField(verMmd.getAbsoluteFieldNumber());
                }
                else
                {
                    // Get the surrogate version from the datastore
                    version = propObj.getProperty(table.getVersionColumn().getName());
                }
                op.setVersion(version);
            }
        }
        return pc;
    }

    public static Object getFieldValueFromStored(ExecutionContext ec, AbstractMemberMetaData mmd, Object value, FieldRole fieldRole) {
        if (value == null)
        {
            return null;
        }

        Class type = value.getClass();
        if (mmd != null)
        {
            if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
            }
            else if (fieldRole == FieldRole.ROLE_ARRAY_ELEMENT)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getArray().getElementType());
            }
            else if (fieldRole == FieldRole.ROLE_MAP_KEY)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
            }
            else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getMap().getValueType());
            }
            else
            {
                type = mmd.getType();
            }
        }

        if (mmd != null && mmd.hasCollection() && fieldRole == FieldRole.ROLE_FIELD)
        {
            Collection<Object> coll;
            try
            {
                Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                coll = (Collection<Object>) instanceType.newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            for (int i=0;i<Array.getLength(value);i++)
            {
                Object elem = Array.get(value, i);
                Object storeElem = getFieldValueFromStored(ec, mmd, elem, FieldRole.ROLE_COLLECTION_ELEMENT);
                coll.add(storeElem);
            }
            return coll;
        }
        else if (mmd != null && mmd.hasArray() && fieldRole == FieldRole.ROLE_FIELD)
        {
            Object array = Array.newInstance(mmd.getType().getComponentType(), Array.getLength(value));
            for (int i=0;i<Array.getLength(value);i++)
            {
                Object elem = Array.get(value, i);
                Object storeElem = getFieldValueFromStored(ec, mmd, elem, FieldRole.ROLE_ARRAY_ELEMENT);
                Array.set(array, i++, storeElem);
            }
            return array;
        }

        if (Byte.class.isAssignableFrom(type) ||
            Boolean.class.isAssignableFrom(type) ||
            Character.class.isAssignableFrom(type) ||
            Double.class.isAssignableFrom(type) ||
            Float.class.isAssignableFrom(type) ||
            Integer.class.isAssignableFrom(type) ||
            Long.class.isAssignableFrom(type) ||
            Short.class.isAssignableFrom(type) ||
            String.class.isAssignableFrom(type))
        {
            return value;
        }
        else if (Enum.class.isAssignableFrom(type))
        {
            ColumnMetaData colmd = null;
            if (mmd != null && mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
            {
                colmd = mmd.getColumnMetaData()[0];
            }
            if (MetaDataUtils.persistColumnAsNumeric(colmd))
            {
                return type.getEnumConstants()[((Number)value).intValue()];
            }
            return Enum.valueOf(type, (String)value);
        }

        TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(type, String.class);
        TypeConverter longConv = ec.getTypeManager().getTypeConverterForType(type, Long.class);
        if (strConv != null)
        {
            // Persisted as a String, so convert back
            String strValue = (String)value;
            return strConv.toMemberType(strValue);
        }
        else if (longConv != null)
        {
            // Persisted as a Long, so convert back
            Long longValue = (Long)value;
            return longConv.toMemberType(longValue);
        }

        // TODO Cater for cases with no converters
        return value;
    }

    public static Object getStoredValueForField(ExecutionContext ec, AbstractMemberMetaData mmd, Object value, FieldRole fieldRole) {
        if (value == null)
        {
            return null;
        }

        Class type = value.getClass();
        if (mmd != null)
        {
            if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
            }
            else if (fieldRole == FieldRole.ROLE_ARRAY_ELEMENT)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getArray().getElementType());
            }
            else if (fieldRole == FieldRole.ROLE_MAP_KEY)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
            }
            else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getMap().getValueType());
            }
            else
            {
                type = mmd.getType();
            }
        }

        if (mmd != null && mmd.hasCollection() && fieldRole == FieldRole.ROLE_FIELD)
        {
            Collection rawColl = (Collection)value;
            if (rawColl.isEmpty())
            {
                return null;
            }

            Object[] objArray = new Object[rawColl.size()];
            int i = 0;
            for (Object elem : rawColl)
            {
                Object storeElem = getStoredValueForField(ec, mmd, elem, FieldRole.ROLE_COLLECTION_ELEMENT);
                objArray[i++] = storeElem;
            }

            // Convert to an accepted array type if necessary
            return convertArrayToStorableArray(objArray, mmd);
        }
        else if (mmd != null && mmd.hasArray() && fieldRole == FieldRole.ROLE_FIELD)
        {
            if (Array.getLength(value) == 0)
            {
                return null;
            }

            if (type.getComponentType().isPrimitive())
            {
                return value;
            }
            else if (type.getComponentType() == String.class)
            {
                return value;
            }

            Object[] objArray = new Object[Array.getLength(value)];
            for (int i=0;i<objArray.length;i++)
            {
                Object elem = Array.get(value, i);
                Object storeElem = getStoredValueForField(ec, mmd, elem, FieldRole.ROLE_ARRAY_ELEMENT);
                objArray[i] = storeElem;
            }

            // Convert to an accepted array type if necessary
            return convertArrayToStorableArray(objArray, mmd);
        }

        if (Byte.class.isAssignableFrom(type) ||
            Boolean.class.isAssignableFrom(type) ||
            Character.class.isAssignableFrom(type) ||
            Double.class.isAssignableFrom(type) ||
            Float.class.isAssignableFrom(type) ||
            Integer.class.isAssignableFrom(type) ||
            Long.class.isAssignableFrom(type) ||
            Short.class.isAssignableFrom(type) ||
            String.class.isAssignableFrom(type))
        {
            // Natively supported
            return value;
        }
        else if (Enum.class.isAssignableFrom(type))
        {
            ColumnMetaData colmd = null;
            if (mmd != null && mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
            {
                colmd = mmd.getColumnMetaData()[0];
            }
            boolean useNumeric = MetaDataUtils.persistColumnAsNumeric(colmd);
            return useNumeric ? ((Enum)value).ordinal() : ((Enum)value).name();
        }

        // Fallback to built-in type converters
        TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(type, String.class);
        TypeConverter longConv = ec.getTypeManager().getTypeConverterForType(type, Long.class);
        if (strConv != null)
        {
            // store as a String
            return strConv.toDatastoreType(value);
        }
        else if (longConv != null)
        {
            // store as a Long
            return longConv.toDatastoreType(value);
        }

        // TODO Cater for cases with no converters
        return value;
    }

    private static Object convertArrayToStorableArray(Object[] objArray, AbstractMemberMetaData mmd) {
        if (objArray == null || objArray.length == 0)
        {
            return null;
        }

        // Convert to an accepted array type if necessary
        Object array = objArray;
        Class cmptCls = objArray[0].getClass();
        if (ClassUtils.isPrimitiveWrapperType(cmptCls.getName()))
        {
            // Primitive wrapper so convert to the primitive array type (ignores null elements)
            Class primType = ClassUtils.getPrimitiveTypeForType(cmptCls);
            array = Array.newInstance(primType, objArray.length);
            for (int i=0;i<objArray.length;i++)
            {
                Array.set(array, i, objArray[i]);
            }
        }
        else if (cmptCls.isPrimitive() || cmptCls == String.class)
        {
            array = Array.newInstance(cmptCls, objArray.length);
            for (int i=0;i<objArray.length;i++)
            {
                Array.set(array, i, objArray[i]);
            }
        }
        else
        {
            throw new NucleusException("Field " + mmd.getFullFieldName() +
                " cannot be persisted to Neo4j since Neo4j doesn't natively support such a type (" + mmd.getType() + ")");
        }

        return array;
    }

    public static OrientElement getPropertyContainerForObjectProvider(OrientGraph db, ObjectProvider op) {
        Object val = op.getAssociatedValue(OrientStoreManager.OBJECT_PROVIDER_PROPCONTAINER);
        if (val != null)
        {
            // Cached with ObjectProvider so return it
            return (OrientElement) val;
        }

        AbstractClassMetaData cmd = op.getClassMetaData();
        ExecutionContext ec = op.getExecutionContext();
        OrientElement propObj = db.getElement(op.getInternalObjectId());
        if (propObj != null)
        {
            // Cache the Node with the ObjectProvider
            op.setAssociatedValue(OrientStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);
        }
        return propObj;
    }

    /**
     * Convenience method to take an object returned by Orient (from a query for example), and prepare it for passing to
     * the user. Makes sure there is a StateManager connected, with associated fields marked as loaded.
     * @param obj The object (from Orient)
     * @param ec execution context
     * @param cont ObjectContainer that returned the object
     * @param cmd ClassMetaData for the object
     * @param mgr OrientStoreManager
     * @return The StateManager for this object
     */
//    public static ObjectProvider prepareOrientObjectForUse(Object obj, ExecutionContext ec, ODatabaseObjectTx cont,
//            AbstractClassMetaData cmd, OrientStoreManager mgr)
//    {
//        if (!ec.getApiAdapter().isPersistable(obj))
//        {
//            return null;
//        }
//
//        ObjectProvider sm = ec.findObjectProvider(obj);
//        if (sm == null)
//        {
//            // Find the identity
//            Object id = null;
//            if (cmd.getIdentityType() == IdentityType.DATASTORE)
//            {
//                ORecordId orid = (ORecordId) cont.getRecordByUserObject(obj, false).getIdentity();
//                String orientId = orid.toString();
//                String[] oidParts = orientId.split(":");
//                String jdoOid = "";
//                jdoOid += oidParts[1];
//                jdoOid += "[OID]";
//                jdoOid += obj.getClass().getName();
//
//                id = OIDFactory.getInstance(ec.getOMFContext(), jdoOid);
//            }
//            else
//            {
//                id = ec.getApiAdapter().getNewApplicationIdentityObjectId(obj, cmd);
//            }
//
//            // Object not managed so give it a StateManager before returning it
//            sm = ObjectProviderFactory.newForPersistentClean(ec, id, obj);
//            sm.provideFields(cmd.getAllMemberPositions(), new AssignStateManagerFieldManager(cont, sm));
//        }
//
//        sm.replaceAllLoadedSCOFieldsWithWrappers();
//
//        return sm;
//    }
}
