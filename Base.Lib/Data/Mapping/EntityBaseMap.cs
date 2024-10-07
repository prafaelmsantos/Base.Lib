namespace Base.Lib.Data.Mapping
{
    public class EntityBaseMap<TEntity> : EntityTypeConfiguration<TEntity> where TEntity : EntityBase
    {
        public override void Map(EntityTypeBuilder<TEntity> entity)
        {
            entity.Property(x => x.Id)
                .HasColumnName("id")
                .ValueGeneratedOnAdd()
                .IsRequired(true);

            entity.HasKey(x => x.Id);
            entity.HasIndex(x => x.Id).IsUnique(true);
        }
    }
}
