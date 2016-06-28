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
package org.datanucleus.store.orient.query;

import java.util.Collections;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.orient.OrientStoreManager;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.util.Localiser;



/**
 * representation of a JPQL query for use by DataNucleus.
 * The query can be specified via method calls, or via a single-string form.
 */
public class JPQLQuery extends AbstractJPQLQuery
{
    {
        Localiser.registerBundle(
            "org.datanucleus.store.orient.Localisation", OrientStoreManager.class.getClassLoader());
    }

    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec) {
        super(storeMgr, ec);
    }

    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, AbstractJPQLQuery q) {
        super(storeMgr, ec, q);
    }

    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, String query) {
        super(storeMgr, ec, query);
    }

    protected Object performExecute(Map parameters)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();

        if (candidateCollection != null && candidateCollection.isEmpty())
        {
            return Collections.EMPTY_LIST;
        }

        boolean inMemory = evaluateInMemory();
        ManagedConnection mconn = ec.getStoreManager().getConnection(ec);
        try
        {
//            ObjectContainer cont = (ObjectContainer) mconn.getConnection();
//
//            // Execute the query
//            long startTime = 0;
//            if (NucleusLogger.QUERY.isDebugEnabled())
//            {
//                startTime = System.currentTimeMillis();
//                NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JPQL", getSingleStringQuery(), null));
//            }
//            List candidates = null;
//            boolean filterInMemory = false;
//            boolean orderingInMemory = false;
//            if (candidateCollection == null)
//            {
//                // Create the SODA query, optionally with the candidate and filter restrictions
//                Query query = createSODAQuery(cont, compilation, parameters, inMemory);
//                candidates = query.execute();
//                if (inMemory)
//                {
//                    filterInMemory = true;
//                    orderingInMemory = true;
//                }
//            }
//            else
//            {
//                candidates = (List)candidateCollection;
//                filterInMemory = true;
//                orderingInMemory = true;
//            }
//
//            // Apply any restrictions to the results (that we can't use in the input SODA query)
//            JavaQueryEvaluator resultMapper =
//                new JPQLEvaluator(this, candidates, compilation, parameters, clr);
//            Collection results = resultMapper.execute(filterInMemory, orderingInMemory, true, true, true);
//
//            if (NucleusLogger.QUERY.isDebugEnabled())
//            {
//                NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JPQL", "" + (System.currentTimeMillis() - startTime)));
//            }
//
//            // Assign StateManagers to any returned objects
//            Iterator iter = results.iterator();
//            while (iter.hasNext())
//            {
//                Object obj = iter.next();
//                AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(obj.getClass(), clr);
//                OrientUtils.prepareOrientObjectForUse(obj, ec, cont, cmd, (OrientStoreManager)ec.getStoreManager());
//            }
//
//            if (type == BULK_DELETE)
//            {
//                iter = results.iterator();
//                while (iter.hasNext())
//                {
//                    Object obj = iter.next();
//                    ec.deleteObject(obj);
//                }
//                return Long.valueOf(results.size());
//            }
//            else if (type == BULK_UPDATE)
//            {
//                throw new NucleusException("Bulk Update is not yet supported");
//            }
//            else
//            {
//                return results;
        }
        finally
        {
            mconn.release();
        }
        return null;//TODO
    }

}
