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

import com.moscona.events.EventPublisher
import com.moscona.util.SafeRunnable
import static com.moscona.test.easyb.TestHelper.*


before_each "scenario", {
  given "an event publisher instance", {
    publisher = new EventPublisher()
  }
  and "that there are no subscribers", {
    publisher.clearSubscribers()
  }
  and "a result object", {
    result = [:]
  }
}

scenario "subscribing to one event with a safe runnable", {
  given "that I subscribe to the server startup event", {
    final trace = result
    publisher.onEvent(EventPublisher.Event.SERVER_START, new SafeRunnable() {
      void runWithPossibleExceptions() {
        trace["called"] = true
      }
    })
  }
  when "the event is published", {
    publisher.publish(EventPublisher.Event.SERVER_START)
  }
  then "the subscriber is called", {
    expected = ["called":true]
    result.shouldBe expected
  }
}

scenario "subscribing to several events", {
  given "that I subscribe to the server startup event", {
    final trace = result
    publisher.onEvent(EventPublisher.Event.SERVER_START, new SafeRunnable() {
      void runWithPossibleExceptions() {
        trace["startup"] = true
      }
    })
  }
  given "that I subscribe to the server shutdown event", {
    final trace = result
    publisher.onEvent(EventPublisher.Event.SERVER_SHUTDOWN_COMPLETE, new SafeRunnable() {
      void runWithPossibleExceptions() {
        trace["shutdown"] = true
      }
    })
  }
  when "the events are published", {
    publisher.publish(EventPublisher.Event.SERVER_START)
    publisher.publish(EventPublisher.Event.SERVER_SHUTDOWN_COMPLETE)
  }
  and "an unrelated event is also published", {
    publisher.publish(EventPublisher.Event.SERVER_ALERT)
  }
  then "the subscriber is called", {
    expected = [startup:true, shutdown:true]
    result.shouldBe expected
  }
}

scenario "attaching transient metadata to events", {
  given "that I subscribe to the server alert event", {
    final trace = result
    publisher.onEvent(EventPublisher.Event.SERVER_ALERT, new SafeRunnable() {
      void runWithPossibleExceptions() {
        trace["called"] = true
        trace["metadata"] = EventPublisher.Event.SERVER_ALERT.metadata
      }
    })
  }
  when "the event is published, with metadata", {
    metadata = new HashMap<String,Object>()
    metadata["message"] = "some message"
    publisher.publish(EventPublisher.Event.SERVER_ALERT, metadata)
  }
  then "the subscriber is called and can retrieve the metadata from the event", {
    expected = ["called":true, metadata:[message:"some message"]]
    result.shouldBe expected
  }
}

scenario "metadata of different event types managed separately", {
  given "that I subscribe to the server alert event", {
    final trace = result
    publisher.onEvent(EventPublisher.Event.SERVER_ALERT, new SafeRunnable() {
      void runWithPossibleExceptions() {
        trace["called"] = true
        trace["metadata"] = EventPublisher.Event.SERVER_ALERT.metadata
      }
    })
  }
  when "the event is published, with metadata", {
    metadata = new HashMap<String,Object>()
    metadata["message"] = "some message"
    publisher.publish(EventPublisher.Event.SERVER_ALERT, metadata)
  }
  and "another event is published with different metadata", {
    metadata = new HashMap<String,Object>()
    metadata["message"] = "another message"
    publisher.publish(EventPublisher.Event.SERVER_START, metadata)
  }
  then "the subscriber is called and gets the correct metadata", {
    expected = ["called":true, metadata:[message:"some message"]]
    result.shouldBe expected
  }
}

scenario "pre-defined event types", {
  then "the SERVER_START event type should exist", {
    ensureDoesNotThrow(Throwable) {
      EventPublisher.Event.SERVER_START
    }
  }
  then "the SERVER_SHUTDOWN_COMPLETE event type should exist", {
    ensureDoesNotThrow(Throwable) {
      EventPublisher.Event.SERVER_SHUTDOWN_COMPLETE
    }
  }
  then "the SERVER_SHUTDOWN_IN_PROGRESS event type should exist", {
    ensureDoesNotThrow(Throwable) {
      EventPublisher.Event.SERVER_SHUTDOWN_IN_PROGRESS
    }
  }
  then "the SERVER_ALERT event type should exist", {
    ensureDoesNotThrow(Throwable) {
      EventPublisher.Event.SERVER_ALERT
    }
  }
  then "the TRADING_OPENED event type should exist", {
    ensureDoesNotThrow(Throwable) {
      EventPublisher.Event.TRADING_OPENED
    }
  }
  then "the TRADING_CLOSED event type should exist", {
    ensureDoesNotThrow(Throwable) {
      EventPublisher.Event.TRADING_CLOSED
    }
  }
  then "the SERVER_STARTING event type should exist", {
    ensureDoesNotThrow(Throwable) {
      EventPublisher.Event.SERVER_STARTING
    }
  }
}