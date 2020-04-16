/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.os;

import java.util.Locale;

public class CpuInfo {

  /* Copied from equivalent op in jnr.ffi.Platform.  We have to have this here as it has to be defined
   * before its (multiple) uses in determineCpu() */
  private static final Locale LOCALE = Locale.ENGLISH;

  /* The remainder of this class is largely based on jnr.ffi.Platform in jnr-ffi version 2.1.10.
   * We copy it manually here in order to avoid introducing an extra dependency merely for the sake of
   * evaluating some system properties.
   *
   * jnr-ffi copyright notice follows:
   *
   * Copyright (C) 2008-2010 Wayne Meissner
   *
   * This file is part of the JNR project.
   *
   * Licensed under the Apache License, Version 2.0 (the "License");
   * you may not use this file except in compliance with the License.
   * You may obtain a copy of the License at
   *
   *    http://www.apache.org/licenses/LICENSE-2.0
   *
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
  /** The supported CPU architectures. */
  public enum Cpu {
    /*
     * <b>Note</b> The names of the enum values are used in other parts of the
     * code to determine where to find the native stub library.  Do NOT rename.
     */

    /** 32 bit legacy Intel */
    I386,

    /** 64 bit AMD (aka EM64T/X64) */
    X86_64,

    /** 32 bit Power PC */
    PPC,

    /** 64 bit Power PC */
    PPC64,

    /** 64 bit Power PC little endian */
    PPC64LE,

    /** 32 bit Sun sparc */
    SPARC,

    /** 64 bit Sun sparc */
    SPARCV9,

    /** IBM zSeries S/390 */
    S390X,

    /** 32 bit MIPS (used by nestedvm) */
    MIPS32,

    /** 32 bit ARM */
    ARM,

    /** 64 bit ARM */
    AARCH64,

    /**
     * Unknown CPU architecture. A best effort will be made to infer architecture specific values
     * such as address and long size.
     */
    UNKNOWN;

    @Override
    public String toString() {
      return name().toLowerCase(LOCALE);
    }
  }

  public static Cpu determineCpu() {
    String archString = System.getProperty("os.arch");
    if (equalsIgnoreCase("x86", archString)
        || equalsIgnoreCase("i386", archString)
        || equalsIgnoreCase("i86pc", archString)
        || equalsIgnoreCase("i686", archString)) {
      return Cpu.I386;
    } else if (equalsIgnoreCase("x86_64", archString) || equalsIgnoreCase("amd64", archString)) {
      return Cpu.X86_64;
    } else if (equalsIgnoreCase("ppc", archString) || equalsIgnoreCase("powerpc", archString)) {
      return Cpu.PPC;
    } else if (equalsIgnoreCase("ppc64", archString) || equalsIgnoreCase("powerpc64", archString)) {
      if ("little".equals(System.getProperty("sun.cpu.endian"))) {
        return Cpu.PPC64LE;
      }
      return Cpu.PPC64;
    } else if (equalsIgnoreCase("ppc64le", archString)
        || equalsIgnoreCase("powerpc64le", archString)) {
      return Cpu.PPC64LE;
    } else if (equalsIgnoreCase("s390", archString) || equalsIgnoreCase("s390x", archString)) {
      return Cpu.S390X;
    } else if (equalsIgnoreCase("aarch64", archString)) {
      return Cpu.AARCH64;
    } else if (equalsIgnoreCase("arm", archString) || equalsIgnoreCase("armv7l", archString)) {
      return Cpu.ARM;
    }

    // Try to find by lookup up in the CPU list
    for (Cpu cpu : Cpu.values()) {
      if (equalsIgnoreCase(cpu.name(), archString)) {
        return cpu;
      }
    }

    return Cpu.UNKNOWN;
  }

  private static boolean equalsIgnoreCase(String s1, String s2) {
    return s1.equalsIgnoreCase(s2)
        || s1.toUpperCase(LOCALE).equals(s2.toUpperCase(LOCALE))
        || s1.toLowerCase(LOCALE).equals(s2.toLowerCase(LOCALE));
  }
}
