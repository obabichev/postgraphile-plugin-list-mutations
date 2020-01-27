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

                        const PrimaryKeyType = TableType.getFields()[primaryKey.name].type;

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

                        memo[fieldName] = fieldWithHooks(fieldName, () => ({
                            description: `Updates list of \`${tableTypeName}\`.`,
                            type: PayloadType,
                            args: {
                                input: {
                                    type: new GraphQLNonNull(InputType),
                                },
                            },
                            async resolve(data, {input}, {pgClient}) {
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

                                const query = sql.query`
                                    update ${sql.identifier(table.namespace.name, table.name)} ${tableTag}
                                    
                                    set ${joinValues(columns.map(column => sql.fragment`${sql.identifier(attrByFieldName[column].name)} = ${subTableTag}.${sql.identifier(attrByFieldName[column].name)}::${sql.identifier(attrByFieldName[column].type.namespaceName, attrByFieldName[column].type.name)}`))}
                            
                                    from ( values ${sqlValues} ) as ${subTableTag} (id, ${sqlColumns})
                                    where ${tableTag}.${sql.identifier(primaryKey.name)} = ${subTableTag}.id::${sql.identifier(primaryKey.type.name)}
                                    returning *;
                                    `;

                                const {text, values} = sql.compile(query);

                                const {
                                    rows,
                                } = await pgClient.query(
                                    text,
                                    values,
                                );

                                return {
                                    clientMutationId: input.clientMutationId,
                                    data: rows,
                                };
                            },
                        }), {});

                        return memo;
                    }, {}));
        },
    );
};

module.exports = UpdateListsPlugin;
