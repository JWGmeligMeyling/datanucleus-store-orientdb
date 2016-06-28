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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.query.expression.DyadicExpression;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Expression.Operator;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.query.expression.SubqueryExpression;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.orient.OrientUtils;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * representation of a JDOQL query for use by DataNucleus. The query can be specified via method calls, or via a
 * single-string form.
 */
public class JDOQLQuery extends AbstractJDOQLQuery
{

    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec) {
        super(storeMgr, ec);
    }

    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, AbstractJDOQLQuery q) {
        super(storeMgr, ec, q);
    }

    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, String query) {
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
        OrientGraph cont = (OrientGraph) mconn.getConnection();
        try
        {
            // Execute the query
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021046", "JDOQL", getSingleStringQuery(), null));
            }
            List candidates = null;
            boolean filterInMemory = false;
            boolean orderingInMemory = false;
            // if (datastoreCompilation == null)
            // {
            // Create the SODA query optionally with the candidate and filter restrictions
            OSQLSynchQuery query = createSQLQuery(cont, compilation, parameters, inMemory);
            OrientDynaElementIterable result = cont.command(new OCommandSQL(query.getText())).execute();

            AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, ec.getClassLoaderResolver());
            candidates = new ArrayList();

            for (Object obj : result) {
//                candidates.add(obj);
                candidates.add(OrientUtils.getObjectForPropertyContainer((OrientElement) obj, cmd, ec, !query.isUseCache()));
            }

            if (inMemory)
            {
                filterInMemory = true;
                orderingInMemory = true;
            }
            // }
            // else
            // {
            // candidates = new ArrayList(candidateCollection);
            // filterInMemory = true;
            // orderingInMemory = true;
            // }

            // Apply any restrictions to the results (that we can't use in the input SODA query)
            JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this, candidates, compilation, parameters, clr);
            Collection results = resultMapper.execute(filterInMemory, orderingInMemory, true, true, true);

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021074", "JDOQL", "" + (System.currentTimeMillis() - startTime)));
            }

            // Assign StateManagers to any returned objects
            Iterator iter = results.iterator();
//            while (iter.hasNext())
//            {
//                Object obj = iter.next();
//                AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(obj.getClass(), clr);
//                OrientUtils.prepareOrientObjectForUse(obj, ec, (ODatabaseObjectTx) cont, cmd, (OrientStoreManager) ec.getStoreManager());
//            }
            return results;
        }
        finally
        {
            mconn.release();
        }

    }

    /**
     * @param cont
     * @param compilation
     * @param parameters
     * @param inMemory
     * @return
     */
    private OSQLSynchQuery createSQLQuery(OrientGraph cont, QueryCompilation compilation, Map parameters, boolean inMemory)
    {
       
        Expression where = compilation.getExprFilter();

        StringBuffer sqlQuery = new StringBuffer("select from ");
        if (this.subclasses)
        {
//            sqlQuery.append("@");//TODO
        }
        
        Class candidateClass = compilation.getCandidateClass();
//      ((OrientStoreManager)ec.getStoreManager()).registerClassInOrient(cont, candidateClass);
        String table = ec.getMetaDataManager().getMetaDataForClass(candidateClass, ec.getClassLoaderResolver()).getTable();

        sqlQuery.append(table);

        if (where != null)
        {
            sqlQuery.append(" where ");
            sqlQuery.append(parseExpression(where, parameters));

        }
        // TODO in memory
        OSQLSynchQuery query = new OSQLSynchQuery(sqlQuery.toString());

        return query;
    }

    private String parseOperator(Operator operator)
    {
        return operator.toString();
    }

    private String parseExpression(Expression exp, Map parameters)
    {
        StringBuffer result = new StringBuffer();
        if (exp instanceof PrimaryExpression)
        {
            boolean first = true;
            for (String tuple : ((PrimaryExpression) exp).getTuples())
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    result.append(".");
                }
                result.append(tuple);
            }
            return result.toString();
        }
        else if (exp instanceof DyadicExpression)
        {
//            result.append("(");
            result.append(parseExpression(exp.getLeft(), parameters));
            result.append(" ");
            result.append(parseOperator(exp.getOperator()));
            result.append(" ");
            result.append(parseExpression(exp.getRight(), parameters));
//            result.append(")");
            return result.toString();
        }
        else if (exp instanceof Literal)
        {
            Object key = parameters.get(((Literal) exp).getLiteral());
            if (key == null)
            {
                return "null";
            }
            Object val = parameters.get(key);
            return objectToStringValue(val);
        } else if (exp instanceof ParameterExpression)
        {
            String paramName = exp.getSymbol().getQualifiedName();
           
            Object val = parameters.get(paramName);
            return objectToStringValue(val);
        }
        else if (exp instanceof SubqueryExpression)
        {
            // TODO
            throw new NucleusUserException("unsupported expression: " + exp);
        }
        else
        {
            throw new NucleusUserException("unsupported expression: " + exp);
        }
    }

    private String objectToStringValue(Object val)
    {
        if (val == null)
        {
            return "null";
        }
        else if (val.getClass().isPrimitive())
        {
            return val.toString();
        }
        return "'" + val + "'";
    }
}
