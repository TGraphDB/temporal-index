
package org.act.temporal_index.util;


import com.google.common.base.Preconditions;

import static com.google.common.base.Charsets.UTF_8;
import static org.act.temporal_index.util.SizeOf.*;

/**
 * InternalKey是将一个动态属性的点/边id，属性id，某个时间以及相关控制位编码为一个键值对中的键的机制。在设计文档中，我们将一个动态属性的数据分为多个record，而InternalKey就
 * 是一个record的key
 */
public class InternalKey
{
	/**
	 * 点/边id，和属性id的编码，用于确定一个动态属性
	 */
    private final Slice Id;
    /**
     * 一个动态属性某个值的起始时间
     */
    private final int startTime;
    /**
     * 值的长度
     */
    private final int valueLength;
    /**
     * 此key对应的record类型
     */
    private final ValueType valueType;

    
    /**
     * 新建一个InternalKey，将相关信息传入，用于编码后生成一个Slice
     * @param Id
     * @param startTime
     * @param valueLength
     * @param valueType
     */
    public InternalKey(Slice Id, int startTime, int valueLength, ValueType valueType)
    {
        Preconditions.checkNotNull(Id, "userKey is null");
        Preconditions.checkArgument(startTime >= 0, "sequenceNumber is negative");
        Preconditions.checkNotNull(valueType, "valueType is null");
        Preconditions.checkArgument( valueLength >= 0 , "valueLength is nagative" );

        this.Id = Id;
        this.startTime = startTime;
        this.valueType = valueType;
        this.valueLength = valueLength;
    }

    /**
     * 新建一个InternalKey，将相关Slice传入，可用于解码相关信息。如起始时间，record类型，id等
     * @param data
     */
    public InternalKey(Slice data)
    {
        Preconditions.checkNotNull(data, "data is null");
        Preconditions.checkArgument(data.length() >= SIZE_OF_LONG, "data must be at least %s bytes", SIZE_OF_LONG);
        this.Id = getId( data );
        long packedSequenceAndType = data.getLong( data.length() - SIZE_OF_LONG );
        this.startTime = (int)SequenceNumber.unpackTime(packedSequenceAndType);
        this.valueType = SequenceNumber.unpackValueType(packedSequenceAndType);
        this.valueLength = SequenceNumber.unpackValueLength( packedSequenceAndType );
    }

    /**
     * 新建一个InternalKey，将相关byte数组传入，可用于解码相关信息。如起始时间，record类型，id等
     * @param data
     */
    public InternalKey(byte[] data)
    {
        this(Slices.wrappedBuffer(data));
    }

    /**
     * 返回唯一确定某个动态属性的标识，其中其点/边id保存在返回值的前8位，属性id保存在返回值的后4位。
     * @return 唯一确定某个动态属性的标识
     */
    public Slice getId()
    {
        return Id;
    }

    /**
     * 
     * @return 返回此key对应的值的长度
     */
    public int getValueLength()
    {
        return valueLength;
    }
    
    /**
     * @return 返回此key对应值的起始有效时间
     */
    public int getStartTime()
    {
        return startTime;
    }

    /**
     * @return 返回此key对应record的类型
     */
    public ValueType getValueType()
    {
        return valueType;
    }

    /**
     * 用于将相关信息编码为一个Slice
     * @return 编码后的Slice
     */
    public Slice encode()
    {
        Slice slice = Slices.allocate(Id.length() + SIZE_OF_LONG );
        SliceOutput sliceOutput = slice.output();
        sliceOutput.writeBytes(Id);
        sliceOutput.writeLong(SequenceNumber.packSequenceAndValueType(startTime, valueLength, valueType));
        return slice;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InternalKey that = (InternalKey) o;

        if (startTime != that.startTime) {
            return false;
        }
        if (Id != null ? !Id.equals(that.Id) : that.Id != null) {
            return false;
        }
        if (valueType != that.valueType) {
            return false;
        }

        return true;
    }

    private int hash;

    @Override
    public int hashCode()
    {
        if (hash == 0) {
            int result = Id != null ? Id.hashCode() : 0;
            result = 31 * result + (int) (startTime ^ (startTime >>> 32));
            result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
            if (result == 0) {
                result = 1;
            }
            hash = result;
        }
        return hash;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("InternalKey");
        sb.append("{id=").append(getId().getLong( 0 ));
        sb.append( " proId=" ).append( getId().getInt( 8 ) );
        sb.append(", time=").append(getStartTime());
        sb.append(", valueType=").append(getValueType());
        sb.append('}');
        return sb.toString();
    }

    private static Slice getId(Slice data)
    {
        return data.slice(0, 12);
    }
}
