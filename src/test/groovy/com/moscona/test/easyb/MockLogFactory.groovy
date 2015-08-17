/*
 * Copyright (c) 2015. Arnon Moscona
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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

