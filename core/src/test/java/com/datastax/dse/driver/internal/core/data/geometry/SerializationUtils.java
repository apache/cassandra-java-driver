/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.data.geometry;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializationUtils {

  public static Object serializeAndDeserialize(Geometry geometry)
      throws IOException, ClassNotFoundException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);

    out.writeObject(geometry);

    byte[] bytes = baos.toByteArray();
    if (!(geometry instanceof Distance)) {
      byte[] wkb = Bytes.getArray(geometry.asWellKnownBinary());
      assertThat(bytes).containsSequence(wkb);
    }
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
    return in.readObject();
  }
}
