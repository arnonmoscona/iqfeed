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

package com.moscona.trading.adapters.iqfeed;

/**
 * Created by arnon on 10/2/2014.
 * Refactored out of DTNIqfeedHistoricalClient
 */
public class IQFeedError extends Exception {
    private String literalError;
    private boolean isRetriable = true;

    public IQFeedError(String literalError, String message, boolean isRetriable) {
        super(message);
        this.literalError = literalError;
        this.isRetriable = isRetriable;
    }

    public String getLiteralError() {
        return literalError;
    }

    public boolean isRetriable() {
        return isRetriable;
    }
}