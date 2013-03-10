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


def replace_pk(cls):
    """
    A class decorator for transitioning models to ShardedAutoField.  This has
    to handle all the logic that a field may perform on a class (via
    contribute_to_class or otherwise).
    """
    from sqlshards.db.shards.models import ShardedAutoField
    def _replace_pk(model):
        # Undo work from previous add_field/setup_pk run.
        if model._meta.pk:
            model._meta.local_fields.remove(model._meta.pk)
            model._meta.pk = None

        model._meta.has_auto_field = False
        model._meta.auto_field = None

        field = ShardedAutoField(primary_key=True, auto_created=True)
        field.contribute_to_class(model, 'id')

    if cls._shards.is_master:
        for child in cls._shards.nodes:
            child = _replace_pk(child)

    _replace_pk(cls)

    return cls


#: Returns ``True`` if the given class is a partitioned model.
is_partitioned = lambda cls: hasattr(cls, '_shards')

