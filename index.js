const UpdateListsPlugin = (builder, {pgInflection: inflection}) => {
    builder.hook(
        'GraphQLObjectType:fields',
        (
            fields,
            {
                extend,
                getTypeByName,
                newWithHooks,
                pgIntrospectionResultsByKind,
                pgSql: sql,
                gql2pg,
                graphql: {
                    GraphQLObjectType,
                    GraphQLInputObjectType,
                    GraphQLNonNull,
                    GraphQLString,
                    GraphQLList,
                },
            },
            {scope: {isRootMutation}, fieldWithHooks},
        ) => {
            if (!isRootMutation) {
                return fields;
            }

            return extend(
                fields,
                pgIntrospectionResultsByKind.class
                    .filter(table => !!table.namespace)
                    .filter(table => table.isSelectable)
                    .filter(table => table.isInsertable)
                    .filter(table => table.isUpdatable)
                    .reduce((memo, table) => {
                        const Table = getTypeByName(
                            inflection.tableType(table.name, table.namespace.name),
                        );

                        if (!Table) {
                            return memo;
                        }

                        const attributes = pgIntrospectionResultsByKind.attribute
                            .filter(attr => attr.classId === table.id);

                        const primaryKeyConstraint = pgIntrospectionResultsByKind.constraint
                            .filter(con => con.classId === table.id)
                            .filter(con => con.type === 'p')[0];

                        const primaryKey = primaryKeyConstraint &&
                            primaryKeyConstraint.keyAttributeNums.map(
                                num => attributes.filter(attr => attr.num === num)[0],
                            )[0];

                        if (!primaryKey) {
                            return memo;
                        }

                        const BigInt = getTypeByName('BigInt');

                        const TableInput = getTypeByName(inflection.patchType(Table.name));
                        if (!TableInput) {
                            return memo;
                        }

                        const tableTypeName = inflection.tableType(table.name, table.namespace.name);

                        const InputTypeListItem = newWithHooks(
                            GraphQLInputObjectType,
                            {
                                name: `Update${tableTypeName}ItemInput`,
                                description: `All input for the updating \`${tableTypeName}\` item.`,
                                fields: {
                                    id: {
                                        description: primaryKey.name,
                                        type: BigInt,
                                    },
                                    patch: {
                                        description: `The \`${tableTypeName}\` item will updated by this mutation.`,
                                        type: new GraphQLNonNull(TableInput),
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
                                    [inflection.tableName(table.name, table.namespace.name) + 's']: {
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
                                    const tableName = inflection.tableName(
                                        table.name,
                                        table.namespace.name,
                                    );
                                    return {
                                        clientMutationId: {
                                            description:
                                                'The exact same `clientMutationId` that was provided in the mutation input, unchanged and unused. May be used by a client to track mutations.',
                                            type: GraphQLString,
                                        },
                                        [tableName]: {
                                            description: `The \`${tableTypeName}\` list that was updated by this mutation.`,
                                            type: new GraphQLList(Table),
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
                                const inputData = input[inflection.tableName(table.name, table.namespace.name) + 's'];

                                const columns = Object.keys(inputData[0].patch);

                                const primaryKeyConstraint = pgIntrospectionResultsByKind.constraint
                                    .filter(con => con.classId === table.id)
                                    .filter(con => con.type === 'p')[0];

                                const primaryKeys =
                                    primaryKeyConstraint &&
                                    primaryKeyConstraint.keyAttributeNums.map(
                                        num => pgIntrospectionResultsByKind.attribute
                                            .filter(attr => attr.classId === table.id)
                                            .filter(attr => attr.num === num)[0],
                                    );

                                const primaryKey = primaryKeys[0];

                                const attributes = pgIntrospectionResultsByKind.attribute
                                    .filter(attr => attr.classId === table.id)
                                    .filter(attr => columns.indexOf(attr.name) >= 0);

                                const sqlColumns = [];
                                const sqlValues = [];
                                inputData.forEach(data => {
                                    sqlValues.push([gql2pg(data.id, primaryKey.type)]);
                                });

                                const typeCasts = {};

                                attributes.forEach(attr => {
                                    const field = inflection.column(
                                        attr.name,
                                        table.name,
                                        table.namespace.name,
                                    );

                                    sqlColumns.push(sql.identifier(attr.name));

                                    inputData.forEach((data, index) => {
                                        const val = data.patch[field];

                                        if (attr.type.id === '1700') {
                                            typeCasts[field] = 'numeric';
                                        }
                                        if (attr.type.id === '114') {
                                            typeCasts[field] = 'json';
                                        }

                                        sqlValues[index].push(gql2pg(val, attr.type));
                                    });
                                });

                                const joinValues = values => sql.join(values, ', ');

                                const typeCast = column => typeCasts[column.names[0]];

                                const query = sql.query`
                                    update ${sql.identifier(table.namespace.name, table.name)} t
                                    set ${sql.join(sqlColumns.map(col => typeCast(col)
                                    ? sql.fragment`${col} = s.${col}::${sql.identifier(typeCast(col))}`
                                    : sql.fragment`${col} = s.${col}`), ', ')}
                                    from ( values 
                                            ${joinValues(sqlValues.map(val => sql.fragment`(${joinValues(val)})`))}
                                        ) as s (id, ${joinValues(sqlColumns)})
                                    where t.id = s.id::bigint
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
