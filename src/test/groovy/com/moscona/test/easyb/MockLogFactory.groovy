package com.moscona.test.easyb

import org.apache.commons.logging.Log
import org.mockito.Mockito

/**
 * Created: Mar 31, 2010 5:29:01 PM
 * By: Arnon Moscona
 * A way to produce a silent logger for everything
 */
class MockLogFactory extends org.apache.commons.logging.impl.LogFactoryImpl {
    public Log getInstance(java.lang.Class clazz) {
      return Mockito.mock(Log.class)
    }

    public Log getInstance(java.lang.String clazz) {
      return Mockito.mock(Log.class)
    }

    public static Log getLog(java.lang.Class clazz) {
      return Mockito.mock(Log.class)
    }

    public static Log getLog(java.lang.String clazz) {
      return Mockito.mock(Log.class)
    }
}

