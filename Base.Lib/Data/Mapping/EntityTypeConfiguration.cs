namespace Base.Lib.Data.Mapping
{
    public abstract class EntityTypeConfiguration<TEntity> where TEntity : class
    {
        public abstract void Map(EntityTypeBuilder<TEntity> entity);
    }
}
