/*
 * Copyright 2018-2021 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.convert.join;

import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_CAS;
import static org.springframework.data.couchbase.core.support.TemplateUtils.SELECT_ID;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.FetchType;
import org.springframework.data.couchbase.core.query.HashSide;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.core.query.N1qlJoin;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.repository.query.StringBasedN1qlQueryParser;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import com.couchbase.client.java.query.QueryOptions;

/**
 * N1qlJoinResolver resolves by converting the join definition to query statement and executing using CouchbaseTemplate
 *
 * @author Subhashni Balakrishnan
 */
public class N1qlJoinResolver {
	private static final Logger LOGGER = LoggerFactory.getLogger(N1qlJoinResolver.class);

	public static String buildQuery(ReactiveCouchbaseTemplate template, String collectionName,
			N1qlJoinResolverParameters parameters) {
		String joinType = "JOIN";
		String selectEntity = "SELECT META(rks).id AS " + SELECT_ID + ", META(rks).cas AS " + SELECT_CAS + ", (rks).* ";

		StringBuilder useLKSBuilder = new StringBuilder();
		if (parameters.getJoinDefinition().index().length() > 0) {
			useLKSBuilder.append("INDEX(" + parameters.getJoinDefinition().index() + ")");
		}
		String useLKS = useLKSBuilder.length() > 0 ? "USE " + useLKSBuilder.toString() + " " : "";

		String from = "FROM `" + template.getBucketName() + "` lks " + useLKS + joinType + " `" + template.getBucketName()
				+ "` rks";

		StringBasedN1qlQueryParser.N1qlSpelValues n1qlL = Query.getN1qlSpelValues(template, collectionName,
				parameters.getEntityTypeInfo().getType(), parameters.getEntityTypeInfo().getType(), false, null, null);
		String onLks = "lks." + n1qlL.filter;

		StringBasedN1qlQueryParser.N1qlSpelValues n1qlR = Query.getN1qlSpelValues(template, collectionName,
				parameters.getAssociatedEntityTypeInfo().getType(), parameters.getAssociatedEntityTypeInfo().getType(), false,
				null, null);
		String onRks = "rks." + n1qlR.filter;

		StringBuilder useRKSBuilder = new StringBuilder();
		if (parameters.getJoinDefinition().rightIndex().length() > 0) {
			useRKSBuilder.append("INDEX(" + parameters.getJoinDefinition().rightIndex() + ")");
		}
		if (!parameters.getJoinDefinition().hashside().equals(HashSide.NONE)) {
			if (useRKSBuilder.length() > 0)
				useRKSBuilder.append(" ");
			useRKSBuilder.append("HASH(" + parameters.getJoinDefinition().hashside().getValue() + ")");
		}
		if (parameters.getJoinDefinition().keys().length > 0) {
			if (useRKSBuilder.length() > 0)
				useRKSBuilder.append(" ");
			useRKSBuilder.append("KEYS [");
			String[] keys = parameters.getJoinDefinition().keys();

			for (int i = 0; i < keys.length; i++) {
				if (i != 0)
					useRKSBuilder.append(",");
				useRKSBuilder.append("\"" + keys[i] + "\"");
			}
			useRKSBuilder.append("]");
		}

		String on = "ON " + parameters.getJoinDefinition().on().concat(" AND " + onLks).concat(" AND " + onRks);

		String where = "WHERE META(lks).id=\"" + parameters.getLksId() + "\"";
		where += ((parameters.getJoinDefinition().where().length() > 0) ? " AND " + parameters.getJoinDefinition().where()
				: "");

		StringBuilder statementSb = new StringBuilder();
		statementSb.append(selectEntity);
		statementSb.append(" " + from);
		statementSb.append((useRKSBuilder.length() > 0 ? " USE " + useRKSBuilder.toString() : ""));
		statementSb.append(" " + on);
		statementSb.append(" " + where);
		return statementSb.toString();
	}

	public static <R> List<R> doResolve(ReactiveCouchbaseTemplate template, String collectionName,
			N1qlJoinResolverParameters parameters, Class<R> associatedEntityClass) {

		String statement = buildQuery(template, collectionName, parameters);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Join query executed " + statement);
		}

		N1QLQuery query = new N1QLQuery(N1QLExpression.x(statement), QueryOptions.queryOptions());
		List<R> result = template.findByQuery(associatedEntityClass).matching(query).all().collectList().block();
		return result.isEmpty() ? null : result;
	}

	public static boolean isLazyJoin(N1qlJoin joinDefinition) {
		return joinDefinition.fetchType().equals(FetchType.LAZY);
	}

	public static void handleProperties(CouchbasePersistentEntity<?> persistentEntity,
			ConvertingPropertyAccessor<?> accessor, ReactiveCouchbaseTemplate template, String id) {
		persistentEntity.doWithProperties((PropertyHandler<CouchbasePersistentProperty>) prop -> {
			if (prop.isAnnotationPresent(N1qlJoin.class)) {
				N1qlJoin definition = prop.findAnnotation(N1qlJoin.class);
				TypeInformation type = prop.getTypeInformation().getActualType();
				Class clazz = type.getType();
				N1qlJoinResolver.N1qlJoinResolverParameters parameters = new N1qlJoinResolver.N1qlJoinResolverParameters(
						definition, id, persistentEntity.getTypeInformation(), type);
				if (N1qlJoinResolver.isLazyJoin(definition)) {
					N1qlJoinResolver.N1qlJoinProxy proxy = new N1qlJoinResolver.N1qlJoinProxy(template, parameters);
					accessor.setProperty(prop,
							java.lang.reflect.Proxy.newProxyInstance(List.class.getClassLoader(), new Class[] { List.class }, proxy));
				} else {
					accessor.setProperty(prop, N1qlJoinResolver.doResolve(template, null, parameters, clazz));
				}
			}
		});
	}

	static public class N1qlJoinProxy implements InvocationHandler {
		private final ReactiveCouchbaseTemplate reactiveTemplate;
		private final String collectionName = null;
		private final N1qlJoinResolverParameters params;
		private List<?> resolved = null;

		public N1qlJoinProxy(ReactiveCouchbaseTemplate template, N1qlJoinResolverParameters params) {
			this.reactiveTemplate = template;
			this.params = params;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			if (this.resolved == null) {
				this.resolved = doResolve(this.reactiveTemplate, collectionName, this.params,
						this.params.associatedEntityTypeInfo.getType());
			}
			return method.invoke(this.resolved, args);
		}
	}

	static public class N1qlJoinResolverParameters {
		private N1qlJoin joinDefinition;
		private String lksId;
		private TypeInformation<?> entityTypeInfo;
		private TypeInformation<?> associatedEntityTypeInfo;

		public N1qlJoinResolverParameters(N1qlJoin joinDefinition, String lksId, TypeInformation<?> entityTypeInfo,
				TypeInformation<?> associatedEntityTypeInfo) {
			Assert.notNull(joinDefinition, "The join definition is required");
			Assert.notNull(entityTypeInfo, "The entity type information is required");
			Assert.notNull(associatedEntityTypeInfo, "The associated entity type information is required");

			this.joinDefinition = joinDefinition;
			this.lksId = lksId;
			this.entityTypeInfo = entityTypeInfo;
			this.associatedEntityTypeInfo = associatedEntityTypeInfo;
		}

		public N1qlJoin getJoinDefinition() {
			return joinDefinition;
		}

		public String getLksId() {
			return lksId;
		}

		public TypeInformation getEntityTypeInfo() {
			return entityTypeInfo;
		}

		public TypeInformation getAssociatedEntityTypeInfo() {
			return associatedEntityTypeInfo;
		}
	}
}
