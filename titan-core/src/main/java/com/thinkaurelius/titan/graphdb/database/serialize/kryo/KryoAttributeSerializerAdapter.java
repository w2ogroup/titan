package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeSerializer;

import java.nio.ByteBuffer;

public class KryoAttributeSerializerAdapter<T> extends Serializer<T> {

    private final AttributeSerializer<T> serializer;

    KryoAttributeSerializerAdapter(AttributeSerializer<T> serializer) {
        Preconditions.checkNotNull(serializer);
        this.serializer = serializer;
    }

    @Override
    public T read(Kryo kryo, Input in, Class<T> type) {
        ByteBuffer b = KryoSerializer.getByteBuffer(in);
        T value = serializer.read(b);
        KryoSerializer.updateInputPosition(in,b);
        return value;
    }

    @Override
    public void write(Kryo kryo, Output output, T o) {
        serializer.writeObjectData(new KryoDataOutput(output), o);
    }

}
