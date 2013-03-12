def get_sharded_id_sequence_name(model):
    # XXX: This matches what PostgreSQL would normally generate for a serial
    # type.  This is needed because the old AutoSequenceField sets up some
    # signals which assume the sequence name that @replace_pk overwrites.
    return '%s_%s_seq' % (model._meta.db_table, model._meta.pk.column)


def get_canonical_model(model):
    """
    Accepts a model class, returning the canonical parent model if the model is
    a child of an abstract partitioned model.
    """
    if is_partitioned(model) and model._shards.is_child:
        model = model._shards.parent

    return model


#: Returns ``True`` if the given class is a partitioned model.
is_partitioned = lambda cls: hasattr(cls, '_shards')
