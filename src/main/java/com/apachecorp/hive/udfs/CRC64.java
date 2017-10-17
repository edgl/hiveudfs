package com.apachecorp.hive.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.math.BigInteger;
import java.util.UUID;

/**
 * Not really a CRC64 function per se, but
 * the idea is to create a hash that returns
 * a long
 */
public final class CRC64 extends UDF {
    public Long evaluate(final Text s) {
        if (s == null) { return null; }
        return UUID.nameUUIDFromBytes(s.toString().getBytes()).getMostSignificantBits();
    }
}