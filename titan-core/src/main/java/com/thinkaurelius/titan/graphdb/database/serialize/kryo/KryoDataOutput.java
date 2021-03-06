package com.thinkaurelius.titan.graphdb.database.serialize.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import java.nio.ByteBuffer;

public class KryoDataOutput implements DataOutput {


    private final Output output;
    private final KryoSerializer serializer;
    private final Kryo kryo;

    KryoDataOutput(Output output) {
        this(output,null);
    }

    KryoDataOutput(Output output, KryoSerializer serializer) {
        Preconditions.checkNotNull(output);
        this.output=output;
        this.serializer = serializer;
        if (serializer !=null) kryo = serializer.getKryo();
        else kryo = null;
    }

    public DataOutput putLong(long val) {
        output.writeLong(val);
        return this;
    }

    public DataOutput putInt(int val) {
        output.writeInt(val);
        return this;
    }

    public DataOutput putShort(short val) {
        output.writeShort(val);
        return this;
    }

    public DataOutput putByte(byte val) {
        output.writeByte(val);
        return this;
    }

    public DataOutput putString(String string) {
        output.writeString(string);
        return this;
    }

    public DataOutput writeObject(Object object, Class<?> type) {
        Preconditions.checkArgument(serializer != null, "This DataOutput has not been initialized for object writing!");
        Preconditions.checkArgument(serializer.isValidObject(kryo,object), "Cannot de-/serialize object: %s", object);
        kryo.writeObjectOrNull(output, object, type);
        return this;
    }

    public DataOutput writeObjectNotNull(Object object) {
        Preconditions.checkNotNull(object);
        Preconditions.checkArgument(serializer != null, "This DataOutput has not been initialized for object writing!");
        Preconditions.checkArgument(serializer.isValidObject(kryo,object), "Cannot de-/serialize object: %s", object);
        kryo.writeObject(output, object);
        return this;
    }

    public DataOutput writeClassAndObject(Object object) {
        Preconditions.checkArgument(serializer != null, "This DataOutput has not been initialized for object writing!");
        Preconditions.checkArgument(serializer.isValidObject(kryo,object), "Cannot de-/serialize object: %s", object);
        kryo.writeClassAndObject(output, object);
        return this;
    }

    public ByteBuffer getByteBuffer() {
        return ByteBuffer.wrap(output.getBuffer(),0,output.position());
    }

}
