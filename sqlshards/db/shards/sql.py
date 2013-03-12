import time
from django.conf import settings

next_sharded_id = """CREATE OR REPLACE FUNCTION next_sharded_id(varchar, int, OUT result bigint) AS $$
DECLARE
    sequence_name ALIAS FOR $1;
    shard_id ALIAS FOR $2;

    seq_id bigint;
    now_millis bigint;
BEGIN
    SELECT nextval(sequence_name::regclass) % 1024 INTO seq_id;

    SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;
    result := (now_millis - {our_epoch}) << 23;
    result := result | (shard_id << 10);
    result := result | (seq_id);
END;
$$ LANGUAGE PLPGSQL;""".format(our_epoch=settings.SHARD_EPOCH)
