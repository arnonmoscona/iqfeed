package com.moscona.test.easyb

import com.moscona.util.ITimeHelperDelegate
import com.moscona.util.TimeHelper

/**
 * Created: Apr 9, 2010 11:35:49 AM
 * By: Arnon Moscona
 * Used to simulate time on demand in testing
 */
class MockTime implements ITimeHelperDelegate {
  int millis
  int increment
  Calendar today

  /**
   * Create a mock time and optionally sets the time helper to simulation mode using this mock
   * @param opts
   *   hour - the current hour of the day to start with
   *   resetTimeHelper - if true, sets this delegate as the simulation delegate for TimeHelper
   *   increment - if provided sets the msec increment for each call to now(). defaults to 0
   * @return
   */
  def MockTime(opts=[:]) {
    def hour = opts.hour ?: 10
    millis = hour*3600*1000
    increment = opts.increment ?: 0
    def date = opts.date ? new java.text.SimpleDateFormat("MM/dd/yyyy").parse(opts.date) : new Date()

    today = Calendar.instance
    today.timeZone = TimeHelper.INTERNAL_TIMEZONE
    today.set(date.year+1900, date.month, date.date,0,0,0)
    today.set(Calendar.MILLISECOND,0)

    if(opts.resetTimeHelper) {
      TimeHelper.switchToSimulationMode this
    }
  }

  int now(long lastMidnightInMillis) {
    def retval = millis
    millis += increment
    retval
  }

  Calendar today() {
    today
  }
}
