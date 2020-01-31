/*
  Function viaTemporaryTable() is the copy from postgraphile sources
  It's here to reproduce and debug the problem with resolving data in details in details
 */
async function viaTemporaryTable(
    sql,
    pgClient,
    sqlTypeIdentifier,
    sqlMutationQuery,
    sqlResultSourceAlias,
    sqlResultQuery,
    isPgClassLike = true,
    pgRecordInfo = undefined
) {
    const isPgRecord = pgRecordInfo != null;
    const {outputArgTypes, outputArgNames} = pgRecordInfo || {};

    async function performQuery(pgClient, sqlQuery) {
        // TODO: look into rowMode = 'array'
        const {text, values} = sql.compile(sqlQuery);
        // if (debugSql.enabled) debugSql(text);
        return pgClient.query(text, values);
    }

    if (!sqlTypeIdentifier) {
        // It returns void, just perform the query!
        const {rows} = await performQuery(
            pgClient,
            sql.query`
      with ${sqlResultSourceAlias} as (
        ${sqlMutationQuery}
      ) ${sqlResultQuery}`
        );
        return rows;
    } else {
        /*
         * In this code we're converting the rows to a string representation within
         * PostgreSQL itself, then we can send it back into PostgreSQL and have it
         * re-interpret the results cleanly (using it's own serializer/parser
         * combination) so we should be fairly confident that it will work
         * correctly every time assuming none of the PostgreSQL types are broken.
         *
         * If you have a way to improve this, I'd love to see a PR - but please
         * make sure that the integration tests pass with your solution first as
         * there are a log of potential pitfalls!
         */
        const selectionField = isPgClassLike
            ? /*
         * This `when foo is null then null` check might *seem* redundant, but it
         * is not - e.g. the compound type `(,,,,,,,)::my_type` and
         * `null::my_type` differ; however the former also returns true to `foo
         * is null`. We use this check to coalesce both into the canonical `null`
         * representation to make it easier to deal with below.
         */
            sql.query`(case when ${sqlResultSourceAlias} is null then null else ${sqlResultSourceAlias} end)`
            : isPgRecord
                ? sql.query`array[${sql.join(
                    outputArgNames.map(
                        (outputArgName, idx) =>
                            sql.query`${sqlResultSourceAlias}.${sql.identifier(
                                // According to https://www.postgresql.org/docs/10/static/sql-createfunction.html,
                                // "If you omit the name for an output argument, the system will choose a default column name."
                                // In PG 9.x and 10, the column names appear to be assigned with a `column` prefix.
                                outputArgName !== "" ? outputArgName : `column${idx + 1}`
                            )}::text`
                    ),
                    " ,"
                )}]`
                : sql.query`(${sqlResultSourceAlias}.${sqlResultSourceAlias})::${sqlTypeIdentifier}`;
        const result = await performQuery(
            pgClient,
            sql.query`
      with ${sqlResultSourceAlias} as (
        ${sqlMutationQuery}
      )
      select (${selectionField})::text from ${sqlResultSourceAlias}`
        );
        const {rows} = result;
        const firstNonNullRow = rows.find(row => row !== null);
        // TODO: we should be able to have `pg` not interpret the results as
        // objects and instead just return them as arrays - then we can just do
        // `row[0]`. PR welcome!
        const firstKey = firstNonNullRow && Object.keys(firstNonNullRow)[0];
        const rawValues = rows.map(row => row && row[firstKey]);
        const values = rawValues.filter(rawValue => rawValue !== null);
        const sqlValuesAlias = sql.identifier(Symbol());
        const convertFieldBack = isPgClassLike
            ? sql.query`\
              select (str::${sqlTypeIdentifier}).*
              from unnest((${sql.value(values)})::text[]) str`
            : isPgRecord
                ? sql.query`\
          select ${sql.join(
                    outputArgNames.map(
                        (outputArgName, idx) =>
                            sql.query`\
                    (${sqlValuesAlias}.output_value_list)[${sql.literal(
                                idx + 1
                            )}]::${sql.identifier(
                                outputArgTypes[idx].namespaceName,
                                outputArgTypes[idx].name
                            )} as ${sql.identifier(
                                // According to https://www.postgresql.org/docs/10/static/sql-createfunction.html,
                                // "If you omit the name for an output argument, the system will choose a default column name."
                                // In PG 9.x and 10, the column names appear to be assigned with a `column` prefix.
                                outputArgName !== "" ? outputArgName : `column${idx + 1}`
                            )}`
                    ),
                    ", "
                )}
          from (values ${sql.join(
                    values.map(value => sql.query`(${sql.value(value)}::text[])`),
                    ", "
                )}) as ${sqlValuesAlias}(output_value_list)`
                : sql.query`\
        select str::${sqlTypeIdentifier} as ${sqlResultSourceAlias}
        from unnest((${sql.value(values)})::text[]) str`;
        const {rows: filteredValuesResults} =
            values.length > 0
                ? await performQuery(
                pgClient,
                sql.query`\
              with ${sqlResultSourceAlias} as (
                ${convertFieldBack}
              )
              ${sqlResultQuery}
              `
                )
                : {rows: []};
        const finalRows = rawValues.map(
            rawValue =>
                /*
                 * We can't simply return 'null' here because this is expected to have
                 * come from PG, and that would never return 'null' for a row - only
                 * the fields within said row. Using `__isNull` here is a simple
                 * workaround to this, that's caught by `pg2gql`.
                 */
                rawValue === null ? {__isNull: true} : filteredValuesResults.shift()
        );
        return finalRows;
    }
}


const UpdateListsPlugin = (builder) => {
    builder.hook(
        'GraphQLObjectType:fields',
        (
            fields,
            {
                extend,
                getTypeByName,
                newWithHooks,
                pgIntrospectionResultsByKind: introspectionResultsByKind,
                pgQueryFromResolveData: queryFromResolveData,
                // pgViaTemporaryTable: viaTemporaryTable,
                pgColumnFilter: columnFilter,
                pgSql: sql,
                gql2pg,
                graphql: {
                    GraphQLObjectType,
                    GraphQLInputObjectType,
                    GraphQLNonNull,
                    GraphQLString,
                    GraphQLList,
                },
                inflection,
                parseResolveInfo
            },
            {scope: {isRootMutation}, fieldWithHooks},
        ) => {
            if (!isRootMutation) {
                return fields;
            }

            return extend(
                fields,
                introspectionResultsByKind.class
                    .filter(table => !!table.namespace)
                    .filter(table => table.isSelectable)
                    .filter(table => table.isInsertable)
                    .filter(table => table.isUpdatable)
                    .reduce((memo, table) => {
                        const TableType = getTypeByName(inflection.domainType(table.type));

                        if (!TableType) {
                            return memo;
                        }

                        const attributes = introspectionResultsByKind.attribute
                            .filter(attr => attr.classId === table.id);

                        const attrByFieldName = introspectionResultsByKind.attribute
                            .filter(attr => attr.classId === table.id)
                            // .filter(attr => columnFilter(attr, build, context))
                            .reduce((memo, attr) => {
                                const fieldName = inflection.column(attr);
                                memo[fieldName] = attr;
                                return memo;
                            }, {});

                        const primaryKeyConstraint = introspectionResultsByKind.constraint
                            .filter(con => con.classId === table.id)
                            .filter(con => con.type === 'p')[0];

                        if (!primaryKeyConstraint) {
                            return memo;
                        }

                        const primaryKey = primaryKeyConstraint.keyAttributeNums.map(
                            num => attributes.filter(attr => attr.num === num)[0],
                        )[0];

                        if (!primaryKey) {
                            return memo;
                        }

                        const PrimaryKeyType = TableType.getFields()[primaryKey.name] && TableType.getFields()[primaryKey.name].type || null;

                        if (!PrimaryKeyType) {
                            return memo;
                        }

                        const TablePatchType = getTypeByName(inflection.patchType(TableType.name));
                        if (!TablePatchType) {
                            return memo;
                        }

                        const tableTypeName = inflection.tableType(table);

                        const InputTypeListItem = newWithHooks(
                            GraphQLInputObjectType,
                            {
                                name: `Update${tableTypeName}ItemInput`,
                                description: `All input for the updating \`${tableTypeName}\` item.`,
                                fields: {
                                    id: {
                                        description: primaryKey.name,
                                        type: PrimaryKeyType,
                                    },
                                    patch: {
                                        description: `The \`${tableTypeName}\` item will updated by this mutation.`,
                                        type: new GraphQLNonNull(TablePatchType),
                                    },
                                },
                            },
                            {
                                isPgCreateInputType: false,
                                pgInflection: table,
                            },
                        );

                        const InputType = newWithHooks(
                            GraphQLInputObjectType,
                            {
                                name: `Update${tableTypeName}ListInput`,
                                description: `All input for the updating \`${tableTypeName}\` list.`,
                                fields: {
                                    clientMutationId: {
                                        description:
                                            'An arbitrary string value with no semantic meaning. Will be included in the payload verbatim. May be used to track mutations by the client.',
                                        type: GraphQLString,
                                    },
                                    [inflection.column(table) + 's']: {
                                        description: `The \`${tableTypeName}\` list will updated by this mutation.`,
                                        type: new GraphQLList(InputTypeListItem),
                                    },
                                },
                            },
                            {
                                isPgCreateInputType: false,
                                pgInflection: table,
                            },
                        );

                        const PayloadType = newWithHooks(
                            GraphQLObjectType,
                            {
                                name: `Update${tableTypeName}ListPayload`,
                                description: `The output of updating \`${tableTypeName}\` list mutation.`,
                                fields: () => {
                                    return {
                                        clientMutationId: {
                                            description:
                                                'The exact same `clientMutationId` that was provided in the mutation input, unchanged and unused. May be used by a client to track mutations.',
                                            type: GraphQLString,
                                        },
                                        [inflection.column(table) + 's']: {
                                            description: `The \`${tableTypeName}\` list that was updated by this mutation.`,
                                            type: new GraphQLList(TableType),
                                            resolve(data) {
                                                return data.data;
                                            },
                                        },
                                    };
                                },
                            },
                            {
                                isMutationPayload: true,
                                isPgCreatePayloadType: false,
                                pgIntrospection: table,
                            },
                        );

                        const fieldName = `update${tableTypeName}s`;

                        memo[fieldName] = fieldWithHooks(fieldName, ({getDataFromParsedResolveInfoFragment}) => ({
                            description: `Updates list of \`${tableTypeName}\`.`,
                            type: PayloadType,
                            args: {
                                input: {
                                    type: new GraphQLNonNull(InputType),
                                },
                            },
                            async resolve(data, {input}, {pgClient}, resolveInfo) {
                                const parsedResolveInfoFragment = parseResolveInfo(
                                    resolveInfo
                                );
                                const resolveData = getDataFromParsedResolveInfoFragment(
                                    parsedResolveInfoFragment,
                                    PayloadType
                                );
                                const insertedRowAlias = sql.identifier(Symbol());

                                const _query = queryFromResolveData(
                                    insertedRowAlias,
                                    insertedRowAlias,
                                    resolveData,
                                    {}
                                );

                                const inputData = input[inflection.column(table) + 's'];

                                const columns = Object.keys(inputData[0].patch);

                                const primaryKeyConstraint = introspectionResultsByKind.constraint
                                    .filter(con => con.classId === table.id)
                                    .filter(con => con.type === 'p')[0];

                                const primaryKeys =
                                    primaryKeyConstraint &&
                                    primaryKeyConstraint.keyAttributeNums.map(
                                        num => introspectionResultsByKind.attribute
                                            .filter(attr => attr.classId === table.id)
                                            .filter(attr => attr.num === num)[0],
                                    );

                                const primaryKey = primaryKeys[0];

                                const joinValues = values => sql.join(values, ', ');

                                const sqlColumns = joinValues(columns.map(column => {
                                    return sql.identifier(attrByFieldName[column].name);
                                }));

                                const sqlValues = joinValues(inputData.map(entity => {
                                    return sql.fragment`(${joinValues([gql2pg(entity.id, primaryKey.type), ...columns.map(column => {
                                        return gql2pg(entity.patch[column], attrByFieldName[column].type);
                                    })])})`;
                                }));

                                const tableTag = sql.identifier(Symbol());
                                const subTableTag = sql.identifier(Symbol());

                                const mutationQuery = sql.query`
                                    update ${sql.identifier(table.namespace.name, table.name)} ${tableTag}
                                    set ${joinValues(columns.map(column => sql.fragment`${sql.identifier(attrByFieldName[column].name)} = ${subTableTag}.${sql.identifier(attrByFieldName[column].name)}::${sql.identifier(attrByFieldName[column].type.namespaceName, attrByFieldName[column].type.name)}`))}
                                    from ( values ${sqlValues} ) as ${subTableTag} (id, ${sqlColumns})
                                    where ${tableTag}.${sql.identifier(primaryKey.name)} = ${subTableTag}.id::${sql.identifier(primaryKey.type.name)}
                                    returning *
                                    `;

                                try {
                                    await pgClient.query("SAVEPOINT graphql_mutation");
                                    const _rows = await viaTemporaryTable(
                                        sql,
                                        pgClient,
                                        sql.identifier(table.namespace.name, table.name),
                                        mutationQuery,
                                        insertedRowAlias,
                                        _query
                                    )

                                    const {text, values} = sql.compile(mutationQuery);

                                    const {
                                        rows,
                                    } = await pgClient.query(
                                        text,
                                        values,
                                    );

                                    await pgClient.query(
                                        "RELEASE SAVEPOINT graphql_mutation"
                                    );

                                    return {
                                        clientMutationId: input.clientMutationId,
                                        data: rows,
                                    };
                                } catch (e) {
                                    await pgClient.query(
                                        "ROLLBACK TO SAVEPOINT graphql_mutation"
                                    );
                                    throw e;
                                }
                            },
                        }), {});

                        return memo;
                    }, {}));
        },
    );
};

module.exports = UpdateListsPlugin;
