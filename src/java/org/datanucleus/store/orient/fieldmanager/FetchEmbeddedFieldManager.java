package org.datanucleus.store.orient.fieldmanager;

import com.tinkerpop.blueprints.impls.orient.OrientElement;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jan-Willem Gmelig Meyling
 */
public class FetchEmbeddedFieldManager extends FetchFieldManager {

    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected final List<AbstractMemberMetaData> mmds;

    public FetchEmbeddedFieldManager(ObjectProvider op, OrientElement orientElement, List<AbstractMemberMetaData> mmds, Table table) {
        super(op, orientElement, table);
        this.mmds = mmds;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
        {
            // Special case of this being a link back to the owner. TODO Repeat this for nested and their owners
            ObjectProvider[] ownerOps = ec.getOwnersForEmbeddedObjectProvider(op);
            return (ownerOps != null && ownerOps.length > 0 ? ownerOps[0].getObject() : null);
        }

        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                /*if (relationType == RelationType.ONE_TO_ONE_BI || relationType == RelationType.MANY_TO_ONE_BI)
                {
                    if ((ownerMmd.getMappedBy() != null && embMmd.getName().equals(ownerMmd.getMappedBy())) ||
                        (embMmd.getMappedBy() != null && ownerMmd.getName().equals(embMmd.getMappedBy())))
                    {
                        // Other side of owner bidirectional, so return the owner
                        ObjectProvider[] ownerSms = op.getEmbeddedOwners();
                        if (ownerSms == null)
                        {
                            throw new NucleusException("Processing of " + embMmd.getFullFieldName() + " cannot set value to owner since owner ObjectProvider not set");
                        }
                        return ownerSms[0].getObject();
                    }
                }*/

                // Check for null value (currently need all columns to return null)
                // TODO Cater for null using embmd.getNullIndicatorColumn etc

                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                embMmds.add(mmd);
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                ObjectProvider embOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, embCmd, op, fieldNumber);
                FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embOP, orientElement, embMmds, table);
                embOP.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
                return embOP.getObject();
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported as embedded into the owning object");
            }
            return null;
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }

}
