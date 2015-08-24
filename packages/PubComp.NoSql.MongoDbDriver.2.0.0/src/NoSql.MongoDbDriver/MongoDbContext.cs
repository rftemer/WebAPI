using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using MongoDB.Driver.Linq;
using PubComp.NoSql.Core;

namespace PubComp.NoSql.MongoDbDriver
{
    public abstract class MongoDbContext : IDomainContext
    {
        private readonly MongoDB.Driver.MongoClient innerContext;
        private readonly IEnumerable<IEntitySet> entitySets;
        private readonly IFileSet fileSet;
        private readonly string db;

        public MongoDbContext(MongoDbConnectionInfo connectionInfo)
            : this(connectionInfo.ConnectionString, connectionInfo.Db)
        {
        }

        public MongoDbContext(string connectionString, string dbName)
        {
            var concreteType = this.GetType();

            this.db = dbName;
            this.innerContext = new MongoDB.Driver.MongoClient(connectionString);
            var entitySets = new List<IEntitySet>();

            var entitySetProperties = concreteType.GetProperties()
                                        .Where(p => p.PropertyType.IsGenericType
                                            && p.PropertyType.GetGenericTypeDefinition() == typeof(IEntitySet<,>));

            foreach (var prop in entitySetProperties)
            {
                var keyType = prop.PropertyType.GetGenericArguments()[0];
                var entityType = prop.PropertyType.GetGenericArguments()[1];

                var createEntitySetMethod = typeof(EntitySet<,>).MakeGenericType(new[] { keyType, entityType })
                    .GetConstructor(
                        System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
                        null, new[] { typeof(MongoDbContext), typeof(string) }, null);

                var entitySet = createEntitySetMethod.Invoke(new object[] { this, this.db });
                prop.SetValue(this, entitySet, new object[] { });

                entitySets.Add(entitySet as IEntitySet);
            }

            this.entitySets = entitySets;

            var knownTypesResolverAttrs = concreteType.GetCustomAttributes(typeof(KnownDataTypesResolverAttribute));
            foreach (KnownDataTypesResolverAttribute knownTypesResolverAttr in knownTypesResolverAttrs)
            {
                foreach (var type in knownTypesResolverAttr.TypesToUseForSearchingAssemblies)
                {
                    var concreteEntityTypes =
                        ContextUtils.FindInheritingTypes(type.Assembly, entitySets.Select(s => s.EntityType));

                    RegisterKnownTypes(concreteEntityTypes);
                }
            }

            var knownTypesAttrs = concreteType.GetCustomAttributes(typeof(KnownDataTypesAttribute));
            foreach (KnownDataTypesAttribute knownTypesAttr in knownTypesAttrs)
            {
                RegisterKnownTypes(knownTypesAttr.Types);
            }

            var fileSetProperty = concreteType.GetProperties()
                                        .Where(p => p.PropertyType.IsGenericType
                                            && p.PropertyType.GetGenericTypeDefinition() == typeof(IFileSet<>))
                                        .SingleOrDefault();

            if (fileSetProperty != null)
            {
                var keyType = fileSetProperty.PropertyType.GetGenericArguments()[0];

                var createFileSetMethod = typeof(FileSet<>).MakeGenericType(new[] { keyType })
                    .GetConstructor(
                    System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
                    null, new[] { typeof(MongoDbContext), typeof(string) }, null);

                var fileSet = createFileSetMethod.Invoke(new object[] { this, this.db });
                fileSetProperty.SetValue(this, fileSet, new object[] { });

                this.fileSet = fileSet as IFileSet;
            }
        }

        private void RegisterKnownTypes(Type[] types)
        {
            foreach (var type in types)
                MongoDB.Bson.Serialization.BsonClassMap.LookupClassMap(type);
        }

        public void Dispose()
        {
            // The official driver maintains a connection pool internally.
            // You do not need to dispose of any connections or even establish new connections.
        }

        public IEnumerable<IEntitySet> EntitySets
        {
            get
            {
                return this.entitySets;
            }
        }

        public IEntitySet<TKey, TEntity> GetEntitySet<TKey, TEntity>() where TEntity : class, IEntity<TKey>
        {
            var set = this.entitySets.Where(s => s.KeyType == typeof(TKey) && s.EntityType == typeof(TEntity))
                .FirstOrDefault() as IEntitySet<TKey, TEntity>;

            return set;
        }

        public IFileSet Files
        {
            get
            {
                return this.fileSet;
            }
        }

        public void DeleteAll()
        {
            foreach (var entitySet in this.entitySets)
                (entitySet as EntitySet).DeleteAll();
        }

#if DEBUG
        public void SuperDeleteAll()
        {
            var database = this.innerContext.GetServer().GetDatabase(this.db);
            var sets = database.GetCollectionNames();
            foreach (var set in sets)
                database.GetCollection(set).RemoveAll();
        }
#endif
        public void UpdateIndexes(bool removeStaleIndexes)
        {
            foreach (var set in entitySets)
                (set as EntitySet).UpdateIndexes(removeStaleIndexes);
        }

        internal MongoDB.Driver.MongoClient InnerContext
        {
            get
            {
                return this.innerContext;
            }
        }

        private static MongoDB.Bson.BsonValue IdToKey<TKey>(TKey key)
        {
            var result = MongoDB.Bson.BsonValue.Create(key);
            return result;
        }

        public abstract class EntitySet
        {
            internal abstract void UpdateIndexes(bool removeStaleIndexes);
            public abstract IndexDefinition[] GetIndexes();
            public abstract void DeleteAll();
        }

        public class EntitySet<TKey, TEntity> : EntitySet, IEntitySet<TKey, TEntity>
            where TEntity : class, IEntity<TKey>
        {
            protected static readonly List<PropertyInfo> UpdatableProperties;
            protected readonly MongoDbContext parent;
            protected readonly MongoDB.Driver.MongoCollection<TEntity> innerSet;
            protected readonly Type parentType;

            static EntitySet()
            {
                UpdatableProperties = new List<PropertyInfo>();

                var mapper = MongoDB.Bson.Serialization.BsonClassMap.RegisterClassMap<TEntity>(cm =>
                    {
                        cm.AutoMap();
                    });

                foreach (var prop in typeof(TEntity).GetProperties())
                {
                    if (prop.Name == "Id" || !prop.CanRead || !prop.CanWrite || prop.GetIndexParameters().Any())
                        continue;

                    if (prop.GetCustomAttributes(typeof(DbIgnoreAttribute), true).Any()
                        || prop.GetCustomAttributes(typeof(NavigationAttribute), true).Any())
                    {
                        mapper.UnmapProperty(prop.Name);
                        continue;
                    }

                    if (prop.PropertyType == typeof(DateTime))
                    {
                        var dateOnly = prop.GetCustomAttributes(typeof(DateOnlyAttribute), true).Any();

                        mapper.GetMemberMap(prop.Name)
                            .SetSerializationOptions(
                                new MongoDB.Bson.Serialization.Options.DateTimeSerializationOptions
                                {
                                    //DateOnly = dateOnly, // This doesn't work well
                                    Kind = (dateOnly ? DateTimeKind.Local : DateTimeKind.Utc),
                                    Representation = MongoDB.Bson.BsonType.DateTime,
                                });
                    }

                    UpdatableProperties.Add(prop);
                }
            }

            internal EntitySet(MongoDbContext parent, string db)
            {
                this.parent = parent;
                this.innerSet = parent.innerContext.GetServer().GetDatabase(db).GetCollection<TEntity>(typeof(TEntity).Name.ToLower());
                this.parentType = parent.GetType();
            }

            internal override void UpdateIndexes(bool removeStaleIndexes)
            {
                var indexDefinitions = new List<IndexDefinition>();

                foreach (var prop in parentType.GetProperties(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic))
                {
                    if (!prop.CanRead || prop.PropertyType != typeof(IndexDefinition) || prop.GetIndexParameters().Any())
                        continue;

                    var indexDefinition = prop.GetValue(null, new object[] { }) as IndexDefinition;
                    if (indexDefinition.EntityType != typeof(TEntity))
                        continue;

                    indexDefinitions.Add(indexDefinition);
                }

                if (removeStaleIndexes)
                {
                    var staleIndexes = this.innerSet.GetIndexes().Where(
                        i => i.Name != "_id_" && indexDefinitions.All(d => IsEqualToIndex(d, i) == false)).ToList();

                    foreach (var index in staleIndexes)
                        this.innerSet.DropIndex(index.Key);
                }

                foreach (var indexDefinition in indexDefinitions)
                    this.CreateIndex(indexDefinition);
            }

            private bool IsEqualToIndex(IndexDefinition indexDefintion, MongoDB.Driver.IndexInfo indexInfo)
            {
                var names1 = indexDefintion.Fields.Select(f => f.Name).ToList();
                var names2 = indexInfo.Key.Elements.Select(e => e.Name).ToList();

                if (names1.Count != names2.Count)
                    return false;

                for (int cnt = 0; cnt < names1.Count; cnt++)
                {
                    if (names1[cnt] != names2[cnt])
                        return false;
                }

                return true;
            }

            public Type KeyType
            {
                get
                {
                    return typeof(TKey);
                }
            }

            public Type EntityType
            {
                get
                {
                    return typeof(TEntity);
                }
            }

            public IQueryable<TEntity> AsQueryable()
            {
                return this.innerSet.AsQueryable();
            }

            IQueryable<IEntity> IEntitySet.AsQueryable()
            {
                return this.AsQueryable();
            }

            public bool AddIfNotExists(TEntity entity)
            {
                if (EqualityComparer<TKey>.Default.Equals(entity.Id, default(TKey)))
                    throw new DalNullIdFailure("Could not add entity - entity.Id is undefined.", entity, DalOperation.Add);

                var doesContain = Contains(entity.Id);
                if (doesContain)
                    return false;

                CheckIfCanModify(entity);

                Add(entity);
                return true;
            }

            bool IEntitySet.AddIfNotExists(IEntity entity)
            {
                if (entity is TEntity == false)
                    throw new InvalidOperationFailure();

                return this.AddIfNotExists((TEntity)entity);
            }

            public void AddOrUpdate(TEntity entity)
            {
                if (EqualityComparer<TKey>.Default.Equals(entity.Id, default(TKey)))
                    throw new DalNullIdFailure("Could not add entity - entity.Id is undefined.", entity, DalOperation.Add);

                CheckIfCanModify(entity);

                this.innerSet.Save(entity);
            }

            void IEntitySet.AddOrUpdate(IEntity entity)
            {
                if (entity is TEntity == false)
                    throw new InvalidOperationFailure();

                this.AddOrUpdate((TEntity)entity);
            }

            public TEntity GetOrAdd(TEntity entity)
            {
                if (EqualityComparer<TKey>.Default.Equals(entity.Id, default(TKey)))
                    throw new DalNullIdFailure("Could not add entity - entity.Id is undefined.", entity, DalOperation.Add);

                var existing = Get(entity.Id);

                if (existing != null)
                    return ReturnAfterCheck(existing);

                CheckIfCanModify(entity);
                Add(entity);
                return null;
            }

            IEntity IEntitySet.GetOrAdd(IEntity entity)
            {
                if (entity is TEntity == false)
                    throw new InvalidOperationException();

                return this.GetOrAdd((TEntity)entity);
            }

            public void Add(TEntity entity)
            {
                if (EqualityComparer<TKey>.Default.Equals(entity.Id, default(TKey)))
                    throw new DalNullIdFailure("Could not add entity - entity.Id is undefined.", entity, DalOperation.Add);

                CheckIfCanModify(entity);

                var result = this.innerSet.Insert(entity);
            }

            void IEntitySet.Add(IEntity entity)
            {
                if (entity is TEntity == false)
                    throw new InvalidOperationException();

                this.Add((TEntity)entity);
            }

            public void Add(IEnumerable<TEntity> entities)
            {
                if (!entities.Any())
                    return;

                if (entities.Any(entity => EqualityComparer<TKey>.Default.Equals(entity.Id, default(TKey))))
                    throw new DalNullIdFailure("Could not add entities - entity.Id is undefined for at least one entity.", null, DalOperation.Add);

                CheckIfCanModify(entities);

                this.innerSet.InsertBatch(entities);
            }

            void IEntitySet.Add(IEnumerable<IEntity> entities)
            {
                var typedEntities = entities.OfType<TEntity>().ToList();
                if (typedEntities.Count < entities.Count())
                    throw new InvalidOperationFailure();

                this.Add(typedEntities);
            }

            public TEntity Get(TKey key)
            {
                return ReturnAfterCheck(this.innerSet.FindOneById(IdToKey(key)));
            }

            IEntity IEntitySet.Get(Object key)
            {
                if (key is TKey == false)
                    throw new InvalidOperationFailure();

                return this.Get((TKey)key);
            }

            public bool Contains(TKey key)
            {
                return this.innerSet.Find(GetQueryById(key)).Any();
            }

            bool IEntitySet.Contains(Object key)
            {
                if (key is TKey == false)
                    throw new InvalidOperationFailure();

                return this.Contains((TKey)key);
            }

            public IEnumerable<TEntity> Get(IEnumerable<TKey> keys)
            {
                if (!keys.Any())
                    return new TEntity[0];

                return Filter(this.innerSet.AsQueryable().Where(entity => keys.Contains(entity.Id)));
            }

            IEnumerable<IEntity> IEntitySet.Get(IEnumerable keys)
            {
                var typedKeys = keys.OfType<TKey>().ToList();
                return this.Get(typedKeys);
            }

            public void Update(TEntity entity)
            {
                if (EqualityComparer<TKey>.Default.Equals(entity.Id, default(TKey)))
                    throw new DalNullIdFailure("Could not update entity - entity.Id is undefined.", entity, DalOperation.Update);

                if (!Contains(entity.Id))
                    throw new DalItemNotFoundFailure("Could not update entity - entity not found in DB.", entity, DalOperation.Update);

                CheckIfCanModify(entity);

                UpdateExisting(entity);
            }

            void IEntitySet.Update(IEntity entity)
            {
                if (entity is TEntity == false)
                    throw new InvalidOperationFailure();

                this.Update((TEntity)entity);
            }

            private MongoDB.Driver.IMongoQuery GetQueryById(TKey key)
            {
                return MongoDB.Driver.Builders.Query<TEntity>.EQ(entity => entity.Id, key);
            }

            private MongoDB.Driver.IMongoSortBy GetSortById(TKey key)
            {
                return MongoDB.Driver.Builders.SortBy<TEntity>.Ascending(EntitySet => EntitySet.Id);
            }

            private MongoDB.Driver.IMongoUpdate GetUpdateAllButId(TEntity entity)
            {
                MongoDB.Driver.Builders.UpdateBuilder updateBuilder = null;

                foreach (var prop in UpdatableProperties)
                {
                    var name = prop.Name;
                    var value = MongoDB.Bson.BsonValue.Create(prop.GetValue(entity, new object[] { }));
                    value = value ?? MongoDB.Bson.BsonNull.Value;

                    if (updateBuilder == null)
                    {
                        updateBuilder = MongoDB.Driver.Builders.Update.Set(name, value);
                    }
                    else
                    {
                        updateBuilder.Set(name, value);
                    }
                }

                return updateBuilder;
            }

            private MongoDB.Driver.IMongoUpdate GetUpdateField(TEntity entity, string fieldName)
            {
                var prop = UpdatableProperties.FirstOrDefault(p => p.Name == fieldName);

                if (prop == null)
                    throw new DalFailure("Field " + fieldName + " does not existing in collection");

                var value = MongoDB.Bson.BsonValue.Create(prop.GetValue(entity, new object[] { }));
                value = value ?? MongoDB.Bson.BsonNull.Value;

                var updateBuilder = MongoDB.Driver.Builders.Update.Set(fieldName, value);

                return updateBuilder;
            }

            private MongoDB.Driver.IMongoUpdate GetIncrementField(string fieldName, long increment)
            {
                var prop = UpdatableProperties.FirstOrDefault(p => p.Name == fieldName);

                if (prop == null)
                    throw new DalFailure("Field " + fieldName + " does not existing in collection");

                var updateBuilder = MongoDB.Driver.Builders.Update.Inc(fieldName, increment);

                return updateBuilder;
            }

            private void UpdateExisting(TEntity entity)
            {
                this.innerSet.FindAndModify(
                    new MongoDB.Driver.FindAndModifyArgs
                    {
                        Query = GetQueryById(entity.Id),
                        SortBy = GetSortById(entity.Id),
                        Update = GetUpdateAllButId(entity)
                    });
            }

            public void Update(IEnumerable<TEntity> entities)
            {
                CheckIfCanModify(entities);

                foreach (var entity in entities)
                    Update(entity);
            }

            public void UpdateField(TEntity entity, string fieldName)
            {
                CheckIfCanModify(entity);

                this.innerSet.FindAndModify(
                    new MongoDB.Driver.FindAndModifyArgs
                    {
                        Query = GetQueryById(entity.Id),
                        SortBy = GetSortById(entity.Id),
                        Update = GetUpdateField(entity, fieldName)
                    });
            }

            public void IncrementField(TKey key, string fieldName, long increment)
            {
                if (OnModifying != null)
                {
                    var entity = Get(key);
                    CheckIfCanModify(entity);
                }

                this.innerSet.FindAndModify(
                    new MongoDB.Driver.FindAndModifyArgs
                    {
                        Query = GetQueryById(key),
                        SortBy = GetSortById(key),
                        Update = GetIncrementField(fieldName, increment)
                    });
            }

            void IEntitySet.Update(IEnumerable<IEntity> entities)
            {
                var typedEntities = entities.OfType<TEntity>().ToList();
                if (typedEntities.Count < entities.Count())
                    throw new InvalidOperationFailure();

                this.Update(typedEntities);
            }

            public void Delete(TEntity entity)
            {
                CheckIfCanDelete(entity);
                this.DeleteInner(entity.Id);
            }

            void IEntitySet.Delete(IEntity entity)
            {
                if (entity is TEntity == false)
                    throw new InvalidOperationFailure();

                this.Delete((TEntity)entity);
            }

            public void Delete(IEnumerable<TEntity> entities)
            {
                if (!entities.Any())
                    return;

                CheckIfCanDelete(entities);

                var keys = entities.Select(e => e.Id);
                this.DeleteInner(keys);
            }

            void IEntitySet.Delete(IEnumerable<IEntity> entities)
            {
                var typedEntities = entities.OfType<TEntity>().ToList();
                if (typedEntities.Count < entities.Count())
                    throw new InvalidOperationFailure();

                this.Delete(typedEntities);
            }

            public void Delete(TKey key)
            {
                if (EqualityComparer<TKey>.Default.Equals(key, default(TKey)))
                    throw new DalNullIdFailure("Could not delete entity - Id was null.", null, DalOperation.Delete);

                if (OnDeleting != null)
                {
                    var entity = Get(key);
                    CheckIfCanDelete(entity);
                }

                this.innerSet.Remove(GetQueryById(key));
            }

            private void DeleteInner(TKey key)
            {
                if (EqualityComparer<TKey>.Default.Equals(key, default(TKey)))
                    throw new DalNullIdFailure("Could not delete entity - Id was null.", null, DalOperation.Delete);

                this.innerSet.Remove(GetQueryById(key));
            }

            void IEntitySet.Delete(Object key)
            {
                if (key is TKey == false)
                    throw new InvalidOperationFailure();

                this.Delete((TKey)key);
            }

            public void Delete(IEnumerable<TKey> keys)
            {
                if (!keys.Any())
                    return;

                if (keys.Any(key => EqualityComparer<TKey>.Default.Equals(key, default(TKey))))
                    throw new DalNullIdFailure("Could not delete entities - at least one provided Id was null.", null, DalOperation.Delete);

                if (OnDeleting != null)
                {
                    var entities = Get(keys);
                    CheckIfCanDelete(entities);
                }

                foreach (var key in keys)
                    this.Delete(key);
            }

            private void DeleteInner(IEnumerable<TKey> keys)
            {
                if (!keys.Any())
                    return;

                if (keys.Any(key => EqualityComparer<TKey>.Default.Equals(key, default(TKey))))
                    throw new DalNullIdFailure("Could not delete entities - at least one provided Id was null.", null, DalOperation.Delete);

                foreach (var key in keys)
                    this.Delete(key);

                // Won't work
                //var query = GetQueryByIds(keys);
                ////var entities = this.innerSet.Find(query).ToList();
                //this.innerSet.Remove(query);
            }

            void IEntitySet.Delete(IEnumerable<Object> keys)
            {
                var typedKeys = keys.OfType<TKey>().ToList();
                if (typedKeys.Count < keys.Count())
                    throw new InvalidOperationFailure();

                this.Delete(typedKeys);
            }

            public override void DeleteAll()
            {
                this.innerSet.RemoveAll();
            }

            internal MongoDB.Driver.MongoCollection<TEntity> InnerSet
            {
                get
                {
                    return this.innerSet;
                }
            }

            private void CreateIndex(IndexDefinition indexDefintion)
            {
                var keys = new MongoDB.Driver.Builders.IndexKeysBuilder();
                foreach (var field in indexDefintion.Fields)
                {
                    if (field.Direction == Direction.Ascending)
                        keys.Ascending(field.Name);
                    else
                        keys.Descending(field.Name);
                }

                var options = new MongoDB.Driver.Builders.IndexOptionsBuilder();
                options.SetSparse(indexDefintion.AsSparse);
                options.SetUnique(indexDefintion.AsUnique);

                this.innerSet.CreateIndex(keys, options);
            }

            private void RemoveIndex(IndexDefinition indexDefintion)
            {
                var keys = new MongoDB.Driver.Builders.IndexKeysBuilder();
                foreach (var field in indexDefintion.Fields)
                {
                    if (field.Direction == Direction.Ascending)
                        keys.Ascending(field.Name);
                    else
                        keys.Descending(field.Name);
                }

                this.innerSet.DropIndex(keys);
            }

            public override IndexDefinition[] GetIndexes()
            {
                var indexes = this.innerSet.GetIndexes();

                return indexes.Select(i =>
                    new IndexDefinition(
                        typeof(TEntity),
                        i.Key.Elements.Select(e => new KeyProperty(
                            e.Name,
                            (e.Value > 0 ? Direction.Ascending : Direction.Descending)
                            )).ToArray(),
                        i.IsUnique,
                        i.IsSparse))
                    .ToArray();
            }

            public void Reduce<TResult>(
                Expression<Func<TEntity, bool>> queryExpression,
                string mapFunction, string reduceFunction, string finalizeFunction,
                bool doGetResults, out IEnumerable<TResult> results,
                ReduceStoreMode storeMode = ReduceStoreMode.None, string resultSet = null)
                where TResult : new()
            {
                if (string.IsNullOrEmpty(resultSet))
                    storeMode = ReduceStoreMode.None;

                MongoDB.Driver.MapReduceOutputMode outputMode;
                String outputCollectionName;
                //MongoDB.Driver.MapReduceOutputMode

                switch (storeMode)
                {
                    case ReduceStoreMode.NewSet:
                        outputMode = MongoDB.Driver.MapReduceOutputMode.Replace;
                        outputCollectionName = resultSet;
                        break;
                    case ReduceStoreMode.ReplaceItems:
                        outputMode = MongoDB.Driver.MapReduceOutputMode.Merge;
                        outputCollectionName = resultSet;
                        break;
                    case ReduceStoreMode.Combine:
                        outputMode = MongoDB.Driver.MapReduceOutputMode.Reduce;
                        outputCollectionName = resultSet;
                        break;
                    default:
                        outputMode = MongoDB.Driver.MapReduceOutputMode.Inline;
                        outputCollectionName = null;
                        break;
                }

                var query = new MongoDB.Driver.Builders.QueryBuilder<TEntity>().Where(queryExpression);

                var reductionResults = this.innerSet.MapReduce(
                    new MongoDB.Driver.MapReduceArgs
                    {
                        Query = query,
                        MapFunction = mapFunction,
                        ReduceFunction = reduceFunction,
                        FinalizeFunction = finalizeFunction,
                        OutputMode = outputMode,
                        OutputCollectionName = outputCollectionName
                    });

                results = (doGetResults) ?
                        ((storeMode == ReduceStoreMode.None)
                            ? reductionResults.GetInlineResultsAs<TResult>()
                            : reductionResults.GetResultsAs<TResult>())
                        : null;
            }

            #region Navigation

            public void LoadNavigation(IEnumerable<TEntity> entities, IEnumerable<string> propertyNames)
            {
                ContextUtils.LoadNavigation<TKey, TEntity>(parent, entities, propertyNames);
            }

            public void SaveNavigation(IEnumerable<TEntity> entities, IEnumerable<string> propertyNames)
            {
                ContextUtils.SaveNavigation<TKey, TEntity>(parent, this, entities, propertyNames);
            }

            #endregion

            #region Events

            public event EventHandler<AccessEventArgs<TEntity>> OnModifying;

            public event EventHandler<AccessEventArgs<TEntity>> OnDeleting;

            public event EventHandler<AccessEventArgs<TEntity>> OnGetting;

            private void CheckIfCanModify(TEntity entity)
            {
                bool canAccess;
                CheckIfCanModify(entity, out canAccess);
                if (!canAccess)
                    throw new DalAccessRestrictionFailure("Modify operation for this entity is forbidden.");
            }

            private void CheckIfCanModify(IEnumerable<TEntity> entities)
            {
                if (this.OnModifying == null)
                    return;

                bool canAccess = true;
                foreach (var entity in entities)
                {
                    var args = new AccessEventArgs<TEntity>(entity);
                    this.OnModifying(this, args);
                    if (!args.CanAccess)
                    {
                        canAccess = false;
                        break;
                    }
                }

                if (canAccess)
                    throw new DalAccessRestrictionFailure("Modify operation for this entity is forbidden.");
            }

            private void CheckIfCanModify(TEntity entity, out bool canAccess)
            {
                canAccess = true;
                if (this.OnModifying == null)
                    return;

                var args = new AccessEventArgs<TEntity>(entity);
                this.OnModifying(this, args);
                canAccess = args.CanAccess;
            }

            private void CheckIfCanDelete(TEntity entity)
            {
                bool canAccess;
                CheckIfCanDelete(entity, out canAccess);
                if (!canAccess)
                    throw new DalAccessRestrictionFailure("Modify operation for this entity is forbidden.");
            }

            private void CheckIfCanDelete(IEnumerable<TEntity> entities)
            {
                if (this.OnDeleting == null)
                    return;

                bool canAccess = true;
                foreach (var entity in entities)
                {
                    var args = new AccessEventArgs<TEntity>(entity);
                    this.OnDeleting(this, args);
                    if (!args.CanAccess)
                    {
                        canAccess = false;
                        break;
                    }
                }

                if (canAccess)
                    throw new DalAccessRestrictionFailure("Modify operation for this entity is forbidden.");
            }

            private void CheckIfCanDelete(TEntity entity, out bool canAccess)
            {
                canAccess = true;
                if (this.OnDeleting == null)
                    return;

                var args = new AccessEventArgs<TEntity>(entity);
                this.OnDeleting(this, args);
                canAccess = args.CanAccess;
            }

            private TEntity ReturnAfterCheck(TEntity entity)
            {
                if (this.OnGetting == null)
                    return entity;

                var args = new AccessEventArgs<TEntity>(entity);
                this.OnGetting(this, args);
                if (!args.CanAccess)
                    throw new DalAccessRestrictionFailure("Modify operating for this entity is forbidden.");

                return entity;
            }

            public IEnumerable<TEntity> Filter(IEnumerable<TEntity> entities)
            {
                if (OnGetting == null)
                    return entities;

                var results = new List<TEntity>();
                foreach (var entity in entities)
                {
                    var args = new AccessEventArgs<TEntity>(entity);
                    OnGetting(this, args);
                    if (args.CanAccess)
                        results.Add(entity);
                }

                return results;
            }

            #endregion
        }

        public abstract class FileSet
        {
        }

        public class FileSet<TKey> : FileSet, IFileSet<TKey>
        {
            protected readonly MongoDbContext parent;
            protected readonly MongoDB.Driver.GridFS.MongoGridFS innerFS;

            internal FileSet(MongoDbContext parent, string db)
            {
                this.innerFS = parent.innerContext.GetServer().GetDatabase(db).GridFS;
            }

            public Type KeyType
            {
                get
                {
                    return typeof(TKey);
                }
            }

            public void Store(Stream inputStream, string fileName, TKey id)
            {
                var bsonID = IdToKey(id);
                var gridFsInfo = this.innerFS.Upload(inputStream, id.ToString());
            }

            public void Retreive(Stream outputStream, TKey id)
            {
                var file = this.innerFS.FindOne(id.ToString());
                this.innerFS.Download(outputStream, file);
            }

            public void Store(string inputFilePath, string fileName, TKey id)
            {
                using (var inputStream = new FileStream(inputFilePath, FileMode.Open, FileAccess.Read))
                {
                    Store(inputStream, fileName, id);
                }
            }

            public void Retreive(string outputFilePath, TKey id)
            {
                using (var outputStream = new FileStream(outputFilePath, FileMode.Create, FileAccess.ReadWrite))
                {
                    Retreive(outputStream, id);
                }
            }

            public void Delete(TKey id)
            {
                this.innerFS.Delete(id.ToString());
            }
        }
    }

    public enum ReduceStoreMode { None, NewSet, ReplaceItems, Combine };
}
