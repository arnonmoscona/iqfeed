import org.mockito.Mockito   //FIXME should be replaced by jMockit, probably
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

import com.moscona.util.testing.TestCoordinator

import com.moscona.test.easyb.MockLogFactory
import static com.moscona.test.easyb.TestHelper.*


import com.moscona.trading.adapters.iqfeed.*
import com.moscona.trading.adapters.iqfeed.IqFeedConfig

import static com.moscona.test.easyb.TestHelper.*
import com.moscona.util.IAlertService
import com.moscona.util.monitoring.stats.*
import com.moscona.events.EventPublisher

import com.moscona.util.TimeHelper
//import com.moscona.trading.elements.IRealTimeTradeRecord
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import com.moscona.trading.excptions.MissingSymbolException
import com.moscona.trading.adapters.iqfeed.IDtnIQFeedConfig

import com.moscona.exceptions.InvalidStateException

import com.moscona.trading.persistence.SplitsDb
import com.moscona.test.easyb.MockTime

description "Functionality of the IQFeed client with a mock IQFeed facade"

class MockIQFeedFacade implements IDtnIQFeedFacade {
  def client = null
  def watchList = []
  IDtnIQFeedConfig config = null
  Closure onDailyRequestResponse;
  Closure onFundamentalsRequestResponse
  def sendGetFundamentalsRequestCalls = [:]
  def sendGetDailyDataRequestCalls = [:]
  def delay = null
  def minuteDataResponse = null
  def dayDataResponse = null
  def tickDataResponse = null
  def fromParam=null
  def toParam=null

  void addCall(counter,args) {
    counter[args]==null ? counter[args]=1 : counter[args]++
  }

  String toString() {
    client==null? "FACADE WITH NO CLIENT" : "facade for $client.debugTag"
  }

  void setClient(IDtnIQFeedHistoricalClient client) {
    this.client = client
  }

  def mockImmediateOnConnectionEstablished(callBackClient,delay=null) {
    this.client = callBackClient
    this.delay = delay
  }

  void startIQConnect() {
    if (delay) {
      sleep(delay)
    }
    client.onConnectionEstablished()
  }

  void startIQConnectPassive() {
    println "delay = $delay" // DELETE THIS LINE!!!
    if (delay) {
      sleep(delay)
    }
    println "***************** Responding to startIQConnectPassive()"  // DELETE THIS LINE!!!
    client.onConnectionEstablished()
  }

  void switchToActiveMode() {

  }

  void requestConnect() {

  }

  void stopIQConnect() {

  }

  void startWatchingSymbol(String symbol) {
    if (!watchList.contains(symbol)) {
      watchList << symbol
    }
    sendGetFundamentalsRequest(symbol)
  }

  void stopWatchingSymbol(String symbol) {
    watchList -= symbol
  }

  void onSystemMessage(String[] fields) {

  }

  void onConnectionEstablished() {
  }

  void sendGetDailyDataRequest(String responseTag, String symbol, int maxPoints) {
    addCall(sendGetDailyDataRequestCalls, [responseTag,symbol,maxPoints])
    onDailyRequestResponse.call()
  }

  void sendGetFundamentalsRequest(String symbol) {
    addCall(sendGetFundamentalsRequestCalls, symbol)
    onFundamentalsRequestResponse.call()
  }

  boolean isWatching(String symbol) {
    return watchList.contains(symbol)
  }

  void init(IDtnIQFeedConfig config) {
    this.config = config
  }

  void doEmergencyRestart() throws InvalidStateException {

  }

  void sendMinuteDataRequest(String symbol, String from, String to, String requestId) {
    fromParam = from
    toParam = to
    client.onLookupData(minuteDataResponse.replaceAll("__REQUEST_ID__",requestId))
  }

  void sendTickDataRequest(String symbol, String from, String to, String requestId) {
    fromParam = from
    toParam = to
    client.onLookupData(tickDataResponse.replaceAll("__REQUEST_ID__",requestId))
  }

  void sendDayDataRequest(String symbol, String from, String to, String requestId) {
    // do nothing
  }

  void unwatchAll() {
    // do nothing
  }
}

before "all", {
  given "a market tree", {
    marketTree = loadMarketTreeFixture()  // FIXME review where this is used and fix accordingly
  }
  and "a mock LogFactory", { logFactory = new MockLogFactory() }
}

def mockImmediateOnConnectionEstablished(mockFacade, callBackClient,delay=null) {
  Mockito.when(mockFacade.startIQConnect()).thenAnswer(new Answer() {
    Object answer(InvocationOnMock invocationOnMock) {
      if (delay) {
        Thread.sleep(delay)
      }
      callBackClient.onConnectionEstablished()
    }
  })

  // also when starting passive
  Mockito.when(mockFacade.startIQConnectPassive()).thenAnswer(new Answer() {
    Object answer(InvocationOnMock invocationOnMock) {
      if (delay) {
        Thread.sleep(delay)
      }
      callBackClient.onConnectionEstablished()
    }
  })
}

def waitForQueueToBeEmpty(c, postWaitSleep=10) {
  while (c.rawMessageQueueSize > 0) {
    Thread.sleep(50)
  }
  if (postWaitSleep && postWaitSleep > 0) {
    Thread.sleep(postWaitSleep)
  }
}

def createClient(facade,config) {
  config.componentConfig["DtnIqfeedHistoricalClient"]["IDtnIQFeedFacade"] = facade
  config.marketTree = marketTree  // FIXME get rid of this dependency
  config.masterTree = masterTree  // FIXME get rid of this dependency
  def c = new DtnIqfeedHistoricalClient()
  facade.mockImmediateOnConnectionEstablished(c)
  printExceptions {
    c.init(config)
  }
  c.facade=facade // just to be on the safe side in a testing scenario
  return c
}

before_each "scenario", {
  client = null
  given "an IQFeed facade", {
    facade = Mockito.mock(IDtnIQFeedFacade)
  }
  and "a mock alert service", {
    alerts = []
    alertService = Mockito.mock(IAlertService)
  }
  and "a mock stats service", {
    stats=[:]
    statsService = Mockito.mock(IStatsService)
  }
  and "a temporary directory", {
    tmpDir = tmpDir()
  }
  and "a master tree", {
    masterTree = loadMasterTreeFixture()  // FIXME review where this is used and fix accordingly
  }
  and "a server configuration", {
    config = null
    printExceptions {
      config = new IqFeedConfig(
          //marketTree: marketTree,
          logFactory: logFactory,
          //staleDataThresholdMillis:50000,
          componentConfig: [
              "DtnIqfeedHistoricalClient": [
                  IDtnIQFeedFacade                : facade,
                  preTradingMonitoringStartMinutes: 2, // auto-start before trading hours start
                  connectionTimeoutMillis         : 30000, // for initial connection
                  maxMillisBetweenTicks           : 500, // issue a "IQFeed hiccup" alert if no trades detected withing time frame (roughly - based on heartbeat)
                  mode                            : "active"
              ],
              "LoggingAlertService"      : [
                  append : false,
                  logFile: "#ProjectRoot{}/src/test/tmp/intellitrade_alerts.log"
              ]
          ],
          alertServiceClassName: "com.moscona.test.util.MemoryAlertService",
          statsServiceClassName: "com.moscona.util.monitoring.stats.SimpleStatsService",
          //masterTree: masterTree
      )
    }
    config.initServicesBundle()
  }
  and "an instance of an IQFeed client", {
    client = new DtnIqfeedHistoricalClient()
    client.debugTag="beforeEach"
    mockImmediateOnConnectionEstablished(facade, client)
    printExceptions {
      client.init(config)
    }
  }
  and "a list of clients to shutdown", {
    shutdownList = [client]
  }
  and "a response to a 31 day BIDU request", {
    biduResponse = """31 day BIDU,2010-06-07 00:00:00,74.2299,69.2300,74.0400,69.8100,10261529,0,
31 day BIDU,2010-06-04 00:00:00,76.2390,73.1500,75.6000,73.2000,9173737,0,
31 day BIDU,2010-06-03 00:00:00,77.9700,74.9200,77.3200,76.6640,14230112,0,
31 day BIDU,2010-06-02 00:00:00,76.0000,73.3700,74.6600,75.9800,9430675,0,
31 day BIDU,2010-06-01 00:00:00,75.6000,72.1300,72.8450,73.5600,11880727,0,
31 day BIDU,2010-05-28 00:00:00,74.2000,72.1200,74.2000,73.2100,10417050,0,
31 day BIDU,2010-05-27 00:00:00,73.5100,69.5600,70.4900,73.4999,14046857,0,
31 day BIDU,2010-05-26 00:00:00,72.1000,67.5700,71.7600,67.5900,12467576,0,
31 day BIDU,2010-05-25 00:00:00,69.4550,66.4301,67.1800,69.0800,15888915,0,
31 day BIDU,2010-05-24 00:00:00,73.3500,70.9400,72.4500,71.0050,10665417,0,
31 day BIDU,2010-05-21 00:00:00,71.6300,66.0300,66.1800,70.8525,20719772,0,
31 day BIDU,2010-05-20 00:00:00,69.6200,66.8200,67.6400,67.5800,15770470,0,
31 day BIDU,2010-05-19 00:00:00,71.9800,68.3610,70.2000,70.1300,13591447,0,
31 day BIDU,2010-05-18 00:00:00,75.2500,71.4200,74.7200,71.5700,12507972,0,
31 day BIDU,2010-05-17 00:00:00,75.1000,71.6400,74.1400,73.1800,15387120,0,
31 day BIDU,2010-05-14 00:00:00,74.8099,72.2800,73.9200,73.9800,19573959,0,
31 day BIDU,2010-05-13 00:00:00,82.2900,75.0000,80.9150,75.6400,58874028,0,
31 day BIDU,2010-05-12 00:00:00,78.3600,72.8200,74.2600,78.2060,41843455,0,
31 day BIDU,2010-05-11 00:00:00,71.6670,68.1220,68.4980,71.4170,2099767,0,
31 day BIDU,2010-05-10 00:00:00,69.4790,66.7440,66.7450,69.4780,1724987,0,
31 day BIDU,2010-05-07 00:00:00,67.1230,63.0000,66.6890,63.9490,1706501,0,
31 day BIDU,2010-05-06 00:00:00,69.4600,62.5000,68.5190,66.8020,1737858,0,
31 day BIDU,2010-05-05 00:00:00,69.8880,67.1540,68.4140,68.9530,1436267,0,
31 day BIDU,2010-05-04 00:00:00,70.3000,68.6550,70.2120,69.3000,1290958,0,
31 day BIDU,2010-05-03 00:00:00,71.2000,69.5010,69.6970,70.8990,1042768,0,
31 day BIDU,2010-04-30 00:00:00,71.6450,68.7680,70.8500,68.8960,1495225,0,
31 day BIDU,2010-04-29 00:00:00,71.8000,70.1000,70.7960,70.9870,3691123,0,
31 day BIDU,2010-04-28 00:00:00,62.7890,62.0000,62.6190,62.1380,1610630,0,
31 day BIDU,2010-04-27 00:00:00,63.2000,61.5500,63.1420,62.0110,1294928,0,
31 day BIDU,2010-04-26 00:00:00,65.1500,63.7140,64.8250,64.0850,902733,0,
31 day BIDU,2010-04-23 00:00:00,64.7770,64.0710,64.2450,64.5760,849975,0,
31 day BIDU,!ENDMSG!,"""
  }
}

after_each "scenario", {
  client.stopDaemonThreads()
}

after_each "scenario", {
  TimeHelper.switchToNormalMode()
  shutdownList.each{it.shutdown()}
}

scenario "initialization", {
  given "a fresh facade", {
    // need a separate facade so we don't get double method invocation counting on it
    facade2 = Mockito.mock(IDtnIQFeedFacade)
    config.componentConfig["DtnIqfeedHistoricalClient"]["IDtnIQFeedFacade"] = facade2
  }
  when "I initialize the client with a configuration", {
    client = new DtnIqfeedHistoricalClient()
    mockImmediateOnConnectionEstablished(facade2, client)
    shutdownList << client
  }
  then "initializing it would not cause exceptions", {
    ensureDoesNotThrow(Exception) {
      client.init(config)
    }
  }
  and "the facade should also get initialized with the process", {
    Mockito.verify(facade2, Mockito.times(1)).init(config)
  }
  and "the facade should be associated with the client", {
    Mockito.verify(facade2, Mockito.times(1)).setClient(client)
  }
  and "the facade should be instructed to initialize the connection in passive mode", {
    Mockito.verify(facade2, Mockito.times(1)).startIQConnect()
  }
}

scenario "server shutdown", {
  when "a server shutdown notification is received", {
    client.shutdown();
  }
  then "the facade should be instructed to tear everything down", {
    Mockito.verify(facade, Mockito.times(1)).stopIQConnect()
  }
}


//====================================================================================================================
// Removed streaming level 1 trade data tests
/* The following were removed as they are only for real-time trade feeds. Out of scope for historical-only client
scenario "starting to watch symbols when initialized within trading hours", {
  given "a fresh facade", {
    // need a separate facade so we don't get double method invocation counting on it
    facade2 = Mockito.mock(IDtnIQFeedFacade)
    config.componentConfig["DtnIqfeedHistoricalClient"]["IDtnIQFeedFacade"] = facade2
  }
  and "time helper in simulated mode, with the time at 11am", {
    mockTime = new MockTime(hour:11, increment:100, resetTimeHelper:true)
  }
  when "I create a new client", {
    client = new DtnIqfeedHistoricalClient()
    mockImmediateOnConnectionEstablished(facade2, client)
    shutdownList << client
    client.init(config)
    sleep(200) // let a couple of heartbeats pass
  }
  then "the facade should be instructed to start watching symbols for all market tree stocks", {
    Mockito.verify(facade2, Mockito.times(502)).startWatchingSymbol(Mockito.anyString())
  }
  and "the facade should get an onConnectionEstablished() call", {
    Mockito.verify(facade2, Mockito.times(1)).onConnectionEstablished()
  }
}

scenario "not starting to watch symbols when initialized outside trading hours", {
  given "a fresh facade", {
    // need a separate facade so we don't get double method invocation counting on it
    facade2 = Mockito.mock(IDtnIQFeedFacade)
    config.componentConfig["DtnIqfeedHistoricalClient"]["IDtnIQFeedFacade"] = facade2
  }
  and "time helper in simulated mode, with the time at 11am", {
    mockTime = new MockTime(hour:8, increment:100, resetTimeHelper:true)
  }
  when "I create a new client", {
    client = new DtnIqfeedHistoricalClient()
    mockImmediateOnConnectionEstablished(facade2, client)
    shutdownList << client
    client.init(config)
  }
  then "the facade should be instructed to start watching symbols for all market tree stocks", {
    Mockito.verify(facade2, Mockito.times(0)).startWatchingSymbol(Mockito.anyString())
  }
}

scenario "auto-starting to watch symbols when initialized before trading hours and the trading day is about to begin", {
  given "a fresh facade", {
    // need a separate facade so we don't get double method invocation counting on it
    facade2 = Mockito.mock(IDtnIQFeedFacade)
    config.componentConfig["DtnIqfeedHistoricalClient"]["IDtnIQFeedFacade"] = facade2
  }
  and "time helper in simulated mode, with the time at 11am", {
    mockTime = new MockTime(hour:11, increment:100, resetTimeHelper:true)
  }
  and "the configuration set to start trading at 11:01", {
    config.startTradingTimeStr = "11:01"
  }
  when "I create a new client", {
    client = new DtnIqfeedHistoricalClient()
    mockImmediateOnConnectionEstablished(facade2, client)
    shutdownList << client
    client.init(config)
  }
  and "I wait a bit", {
    Thread.sleep(500);
  }
  then "the facade should be instructed to start watching symbols for all market tree stocks", {
    Mockito.verify(facade2, Mockito.times(502)).startWatchingSymbol(Mockito.anyString())
  }
}

scenario "stopping to watch symbols when the server publishes end of trading day", {
  given "a fresh facade", {
    // need a separate facade so we don't get double method invocation counting on it
    facade2 = Mockito.mock(IDtnIQFeedFacade)
    config.componentConfig["DtnIqfeedHistoricalClient"]["IDtnIQFeedFacade"] = facade2
  }
  and "time helper in simulated mode, with the time at 11am", {
    mockTime = new MockTime(hour:11, increment:100, resetTimeHelper:true)
  }
  when "I create a new client", {
    client = new DtnIqfeedHistoricalClient()
    mockImmediateOnConnectionEstablished(facade2, client)
    shutdownList << client
    client.init(config)
    sleep(200) // let a couple of heartbeats pass
  }
  and "subsequently publish a TRADING_CLOSED event", {
    config.eventPublisher.publish(EventPublisher.Event.TRADING_CLOSED)
  }
  then "the facade should be instructed to stop watching symbols for all market tree stocks", {
    Mockito.verify(facade2, Mockito.times(502)).stopWatchingSymbol(Mockito.anyString())
  }
}
 End of commented out level1 streaming trade data tests */
//====================================================================================================================


scenario "initialization fails if connection not achieved within a configurable timeout", {
  given "a configuration with a short timeout", {
    config.componentConfig["DtnIqfeedHistoricalClient"].connectionTimeoutMillis = 1
  }
  and "a fresh facade", {
    // need a separate facade so we don't get double method invocation counting on it
    facade2 = Mockito.mock(IDtnIQFeedFacade)
    config.componentConfig["DtnIqfeedHistoricalClient"]["IDtnIQFeedFacade"] = facade2
  }
  when "I initialize the client with a configuration, but the confirmation is late", {
    client = new DtnIqfeedHistoricalClient()
    mockImmediateOnConnectionEstablished(facade2, client, 50)
    shutdownList << client
  }
  then "initializing it should cause an exception", {
    ensureThrows(Exception) {
      client.init(config)
    }
  }
}

//**************************************************************************************************************
// receiving messages

scenario "receiving a 'S,STATS,...,Connected,...' message", {
  when "the client gets a stats message indicating that we are connected", {
    client.onLevelOneData("S,STATS,66.112.148.224,60002,500,20,1,0,1,0,May 13 8:41PM,May 13 8:41PM,Connected,4.7.2.0,246737,0.11,0.11,0,0.45,0.45,0,\n")
    waitForQueueToBeEmpty(client)
  }
  then "the connection status should switch to true", {
    client.isConnected().shouldBe true
  }
  and "the max symbols should be 500", {
    client.maxSymbols.shouldBe 500
  }
  and "the watched symbols should be 20", {
    client.watchedSymbols.shouldBe 20
  }
  and "the IQFeed version should match the current version", {
    client.iqFeedVersion.shouldBe client.iqFeedCompatibleVersion // 4.7.0.9
  }
  and "the currentInternetBandwidthConsumption should be 0.11", {
    ensureFloatClose(client.currentInternetBandwidthConsumption, 0.11, 0.001)
  }
  and "the currentLocalBandwidthConsumption should be 0.45", {
    ensureFloatClose(client.currentLocalBandwidthConsumption, 0.45, 0.001)
  }
}

scenario "receiving a 'S,SERVER DISCONNECTED' message", {
  when "the client gets a server disconnected message", {
    client.onAdminData("S,SERVER DISCONNECTED\n")
    waitForQueueToBeEmpty(client)
  }
  then "the client should issue an alert about it", {
    def alerts = client.parserDaemonServiceBundle.alertService.alertsAsStrings
    alerts.size().shouldBe 2
    ensure(alerts[0]) { contains "IQFeed client (parser thread): Lost connection" }
    ensure(alerts[1]) { contains "IQFeed client (parser thread): Trying to reconnect" }
  }
  and "the facade should be instructed to reconnect", {
    Mockito.verify(facade, Mockito.times(1)).requestConnect()
  }
  and "the connection status should change to false", {
    // (after reconnect it may be true but not subbed here)
    client.isConnected().shouldBe false
  }
}


scenario "receiving a 'S,STATS,,,0,0,0,0,0,0,,,Not Connected,0,0,0,0,0,0,0,0,' message", {
  when "the client gets a server disconnected message", {
    client.onAdminData("S,STATS,,,0,0,0,0,0,0,,,Not Connected,0,0,0,0,0,0,0,0,\n")
    waitForQueueToBeEmpty(client)
  }
  then "the client should issue an alert about it", {
    def alerts = client.parserDaemonServiceBundle.alertService.alertsAsStrings
    alerts.size().shouldBe 2
    ensure(alerts[0]){ contains "IQFeed client (parser thread): Lost connection" }
    ensure(alerts[1]){ contains "IQFeed client (parser thread): Trying to reconnect" }
  }
  and "the facade should be instructed to reconnect", {
    Mockito.verify(facade, Mockito.times(1)).requestConnect()
  }
  and "the connection status should change to false", {
    // (after reconnect it may be true but not subbed here)
    client.isConnected().shouldBe false
  }
}

scenario "receiving a 'S,CUST,delayed,' message", {
  when "receiving a CUST message indicating that we are getting a delayed feed", {
    client.onAdminData("S,CUST,delayed,66.112.148.112,60002,8SpAIvE,4.7.2.0,0,INDEX DTNNEWS RT_TRADER COMTEX PRIMEZONE INTERNET_WIRE AMEX NASDAQ NYSE APCMB,,10,QT_API,,")
    waitForQueueToBeEmpty(client)
  }
  then "an alert should be issued that the real time feed is delayed", {
    def alerts = client.parserDaemonServiceBundle.alertService.alertsAsStrings
    alerts.size().shouldBe 1
    ensure(alerts[0]){ contains "IQFeed client (parser thread): getting delayed data instead of real time. Marking feed as unconnected." }
  }
  and "the system should be considered not connected", {
    client.isConnected().shouldBe false
  }
}

scenario "receiving a 'S,CUST,real_time,' message", {
  when "receiving a CUST message indicating that we are getting a delayed feed", {
    client.onAdminData("S,CUST,real_time,66.112.156.224,60002,73QEupj,4.7.2.0,0,INDEX DTNNEWS RT_TRADER COMTEX PRIMEZONE INTERNET_WIRE AMEX NASDAQ NYSE APCMB,,500,QT_API,,")
    waitForQueueToBeEmpty(client)
  }
  then "an alert should be issued that the real time feed is delayed", {
    alerts.size().shouldBe 0
  }
  and "the system should be considered connected", {
    // note that this is true only in the test as it is not actually changing the value
    client.isConnected().shouldBe true
  }
}

scenario "receiving a 'S,CURRENT UPDATE FIELDNAMES,...' message", {
  when "receiving a CURRENT UPDATE FIELDNAMES followed by a list of fields", {
    client.onAdminData("S,CURRENT UPDATE FIELDNAMES,Symbol,Last,Total Volume,Incremental Volume,Bid,Ask,Bid Size,Ask Size,Last Trade Time,Delay")
    waitForQueueToBeEmpty(client)
  }
  then "the field position map should be updated to reflect the new message", {
    client.positionOf("Symbol").shouldBe 1
    client.positionOf("Last").shouldBe 2
    client.positionOf("Total Volume").shouldBe 3
    client.positionOf("Incremental Volume").shouldBe 4
    client.positionOf("Bid").shouldBe 5
    client.positionOf("Ask").shouldBe 6
    client.positionOf("Bid Size").shouldBe 7
    client.positionOf("Ask Size").shouldBe 8
    client.positionOf("Last Trade Time").shouldBe 9
    client.positionOf("Delay").shouldBe 10
  }
  and "the positions of unlisted fields should be -1", {
    client.positionOf("Change").shouldBe(-1)
  }
}

//====================================================================================================================
// Removed streaming level 1 trade data tests

/*
scenario "receiving a 'S,UPDATE FIELDNAMES,...' message"
S,UPDATE FIELDNAMES,Symbol,Exchange ID,Last,Change,Percent Change,Total Volume,Incremental Volume,High,Low,Bid,Ask,Bid Size,Ask Size,Tick,Bid Tick,Range,Last Trade Time,Open Interest,Open,Close,Spread,Strike,Settle,Delay,Market Center,Restricted Code,Net Asset Value,Average Maturity,7 Day Yield,Last Trade Date,(Reserved),Extended Trading Last,Expiration Date,Regional Volume,Net Asset Value 2,Extended Trading Change,Extended Trading Difference,Price-Earnings Ratio,Percent Off Average Volume,Bid Change,Ask Change,Change From Open,Market Open,Volatility,Market Capitalization,Fraction Display Code,Decimal Precision,Days to Expiration,Previous Day Volume,Regions,Open Range 1,Close Range 1,Open Range 2,Close Range 2,Number of Trades Today,Bid Time,Ask Time,VWAP,TickID,Financial Status Indicator,Settlement Date,Trade Market Center,Bid Market Center,Ask Market Center,Trade Time,Available Regions

scenario "receiving a trade (quote) update message message", {
  when "a trade update is received", {
    client.onAdminData("S,CURRENT UPDATE FIELDNAMES,Symbol,Last,Total Volume,Incremental Volume,Bid,Ask,Bid Size,Ask Size,Last Trade Time,Delay")
    client.onLevelOneData("Q,INTC,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:24t,,")
    waitForQueueToBeEmpty(client)
  }
  and "no alerts should have been issued",{
    def alerts = client.parserDaemonServiceBundle.alertService.alertsAsStrings.findAll{ !it.toString().contains("[IQFeed latency]")}
    def count = alerts.size()
    ensure(count){ isEqualTo 0 }
  }
  then "hasNext() should be true", {
    client.hasNext().shouldBe true
  }
  and "next() should retrieve the tick data", {
    IRealTimeTradeRecord rec = client.next()
    rec.symbol.shouldBe "INTC"
    ensureFloatClose(rec.price, 21.6, 0.001)
    rec.quantity.shouldBe 300
    rec.internalTimestamp.shouldBe(((12*60+21)*60+24)*1000)
    // not testing isStale here
  }
  and "the global counter trade ticks counter should be 1", {
    client.tradeTickCounter.shouldBe 1
  }
  and "the global counter non-trade ticks counter should be 0", {
    client.nonTradeTickCounter.shouldBe 0
  }
}

scenario "receiving a bid update message message", {
  when "a trade update is received", {
    client.onAdminData("S,CURRENT UPDATE FIELDNAMES,Symbol,Last,Total Volume,Incremental Volume,Bid,Ask,Bid Size,Ask Size,Last Trade Time,Delay")
    client.onLevelOneData("Q,AMD,8.7400,28953244,100,8.7300,8.7400,3700,7600,11:18:38b,,")
    waitForQueueToBeEmpty(client)
  }
  and "no alerts should have been issued",{
    def count = client.parserDaemonServiceBundle.alertService.alertsAsStrings.size()
    ensure(count){ isEqualTo 0 }
  }
  then "hasNext() should be false", {
    client.hasNext().shouldBe false
  }
  and "next() should retrieve null", {
    IRealTimeTradeRecord rec = client.next()
    rec.shouldBe null
  }
  and "the global counter trade ticks counter should be 0", {
    client.tradeTickCounter.shouldBe 0
  }
  and "the global counter non-trade ticks counter should be 0", {
    client.nonTradeTickCounter.shouldBe 1
  }
}

scenario "receiving a ask update message message", {
  when "a trade update is received", {
    client.onAdminData("S,CURRENT UPDATE FIELDNAMES,Symbol,Last,Total Volume,Incremental Volume,Bid,Ask,Bid Size,Ask Size,Last Trade Time,Delay")
    client.onLevelOneData("Q,AMD,8.7400,28953244,100,8.7300,8.7400,3700,7600,11:18:38a,,")
    waitForQueueToBeEmpty(client)
  }
  and "no alerts should have been issued",{
    def count = client.parserDaemonServiceBundle.alertService.alertsAsStrings.size()
    ensure(count){ isEqualTo 0 }
  }
  then "hasNext() should be false", {
    client.hasNext().shouldBe false
  }
  and "next() should retrieve null", {
    IRealTimeTradeRecord rec = client.next()
    rec.shouldBe null
  }
  and "the global counter trade ticks counter should be 0", {
    client.tradeTickCounter.shouldBe 0
  }
  and "the global counter non-trade ticks counter should be 0", {
    client.nonTradeTickCounter.shouldBe 1
  }
}

scenario "receiving another update message message", {
  when "a trade update is received", {
    client.onAdminData("S,CURRENT UPDATE FIELDNAMES,Symbol,Last,Total Volume,Incremental Volume,Bid,Ask,Bid Size,Ask Size,Last Trade Time,Delay")
    client.onLevelOneData("Q,INTC,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:24t,,")
    client.onLevelOneData("Q,AMD,8.6600,14280541,100,8.6500,8.6600,10900,5000,12:21:31t,,")
    waitForQueueToBeEmpty(client)
  }
  then "no alerts should have been issued",{
    def alerts = client.parserDaemonServiceBundle.alertService.alertsAsStrings.findAll{ !it.toString().contains("[IQFeed latency]")}
    def count = alerts.size()
    ensure(count){ isEqualTo 0 }
  }
  and "hasNext() should be true", {
    client.hasNext().shouldBe true
  }
  and "next() should retrieve the INTC tick data", {
    IRealTimeTradeRecord rec = client.next()
    rec.symbol.shouldBe "INTC"
    ensureFloatClose(rec.price, 21.6, 0.001)
    rec.quantity.shouldBe 300
    rec.internalTimestamp.shouldBe(((12*60+21)*60+24)*1000)
    // not testing isStale here
  }
  and "next() again should retrieve the AMD tick data", {
    IRealTimeTradeRecord rec = client.next()
    rec.symbol.shouldBe "AMD"
    ensureFloatClose(rec.price, 8.66, 0.001)
    rec.quantity.shouldBe 100
    rec.internalTimestamp.shouldBe(((12*60+21)*60+31)*1000)
    // not testing isStale here
  }
  and "the global counter trade ticks counter should be 1", {
    client.tradeTickCounter.shouldBe 2
  }
  and "the global counter non-trade ticks counter should be 0", {
    client.nonTradeTickCounter.shouldBe 0
  }
}

scenario "no new ticks", {
  then "hasNext() should be false", {
    client.hasNext().shouldBe false
  }
}

*/
//====================================================================================================================


// S,FUNDAMENTAL FIELDNAMES,Symbol,Exchange ID,PE,Average Volume,52 Week High,52 Week Low,Calendar Year High,Calendar Year Low,Dividend Yield,Dividend Amount,Dividend Rate,Pay Date,Ex-dividend Date,(Reserved),(Reserved),(Reserved),Short Interest,(Reserved),Current Year EPS,Next Year EPS,Five-year Growth Percentage,Fiscal Year End,(Reserved),Company Name,Root Option Symbol,Percent Held By Institutions,Beta,Leaps,Current Assets,Current Liabilities,Balance Sheet Date,Long-term Debt,Common Shares Outstanding,(Reserved),Split Factor 1,Split Factor 2,(Reserved),Market Center,Format Code,Precision,SIC,Historical Volatility,Security Type,Listed Market,52 Week High Date,52 Week Low Date,Calendar Year High Date,Calendar Year Low Date,Year End Close,Maturity Date,Coupon Rate,Expiration Date,Strike Proce,NAICS
// F,IBM,D,12.9,8620000,134.2500,99.5000,134.2500,116.0000,2.00,0.55,2.60,06/10/2010,05/06/2010,1,7,,9441017,,10.28,11.26,16.89,12,,INTERNATIONAL BUSINESS MACHINE,IBM,64.,0.69,VIB WIB XBY,48935.0,36002.0,03/01/2010,21932.0,1282348,334111,0.50 05/27/1999,0.50 05/28/1997,,n,14,4,3571,22.76,1,7,01/19/2010,07/08/2009,01/19/2010,05/06/2010,130.90,,,,,334111,,
scenario "receiving a 'S,FUNDAMENTAL FIELDNAMES,...' message followed by a FUNDAMENTALS message", {
  when "receiving a FUNDAMENTAL FIELDNAMES message", {
    client.setFundamentalsRequested(true);
    client.onAdminData("S,FUNDAMENTAL FIELDNAMES,Symbol,Exchange ID,PE,Average Volume,52 Week High,52 Week Low,Calendar Year High,Calendar Year Low,Dividend Yield,Dividend Amount,Dividend Rate,Pay Date,Ex-dividend Date,(Reserved),(Reserved),(Reserved),Short Interest,(Reserved),Current Year EPS,Next Year EPS,Five-year Growth Percentage,Fiscal Year End,(Reserved),Company Name,Root Option Symbol,Percent Held By Institutions,Beta,Leaps,Current Assets,Current Liabilities,Balance Sheet Date,Long-term Debt,Common Shares Outstanding,(Reserved),Split Factor 1,Split Factor 2,(Reserved),Market Center,Format Code,Precision,SIC,Historical Volatility,Security Type,Listed Market,52 Week High Date,52 Week Low Date,Calendar Year High Date,Calendar Year Low Date,Year End Close,Maturity Date,Coupon Rate,Expiration Date,Strike Proce,NAICS")
  }
  and "the a FUNDAMENTALS message", {
    client.onLevelOneData("F,IBM,D,12.9,8620000,134.2500,99.5000,134.2500,116.0000,2.00,0.55,2.60,06/10/2010,05/06/2010,1,7,,9441017,,10.28,11.26,16.89,12,,INTERNATIONAL BUSINESS MACHINE,IBM,64.,0.69,VIB WIB XBY,48935.0,36002.0,03/01/2010,21932.0,1282348,334111,0.50 05/27/1999,0.50 05/28/1997,,n,14,4,3571,22.76,1,7,01/19/2010,07/08/2009,01/19/2010,05/06/2010,130.90,,,,,334111,,")
    waitForQueueToBeEmpty(client)
    sleep(100)
  }
  then "the client should be able to report the fundamentals it has", {
    fundamentals = client.getFundamentals("IBM")

    expected = ["D",12.9,8620000,134.2500,99.5000,134.2500,116.0000,2.00,0.55,2.60,
      new SimpleDateFormat("MM/dd/yyy").parse("06/10/2010"),new SimpleDateFormat("MM/dd/yyy").parse("05/06/2010"),
      9441017,10.28,11.26,16.89,12,"INTERNATIONAL BUSINESS MACHINE","IBM",64.0,0.69,"VIB WIB XBY",48935.0,36002.0,
      new SimpleDateFormat("MM/dd/yyy").parse("03/01/2010"),21932.0,1282348,
      "0.50 05/27/1999","0.50 05/28/1997","n",14,4,3571,22.76,1,7,
      new SimpleDateFormat("MM/dd/yyy").parse("01/19/2010"),
      new SimpleDateFormat("MM/dd/yyy").parse("07/08/2009"),
      new SimpleDateFormat("MM/dd/yyy").parse("01/19/2010"),
      new SimpleDateFormat("MM/dd/yyy").parse("05/06/2010"),
      130.90,null,null,null,null,
      334111]

    excludes = ["Average Volume"]
    ["Exchange ID","PE","Average Volume","52 Week High","52 Week Low","Calendar Year High","Calendar Year Low",
      "Dividend Yield","Dividend Amount","Dividend Rate","Pay Date","Ex-dividend Date","Short Interest",
      "Current Year EPS","Next Year EPS","Five-year Growth Percentage",
      "Fiscal Year End","Company Name","Root Option Symbol","Percent Held By Institutions","Beta",
      "Leaps","Current Assets","Current Liabilities","Balance Sheet Date","Long-term Debt","Common Shares Outstanding",
      "Split Factor 1","Split Factor 2","Market Center","Format Code","Precision","SIC",
      "Historical Volatility","Security Type","Listed Market","52 Week High Date","52 Week Low Date",
      "Calendar Year High Date","Calendar Year Low Date","Year End Close","Maturity Date","Coupon Rate",
      "Expiration Date","Strike Proce","NAICS"].eachWithIndex { field, i->
      if (!excludes.contains(field)) {
        def actualValue = (fundamentals[field].toString()  =~ /[ 0.]*$/).replaceAll("")
        def expectedValue = (expected[i].toString()  =~ /[ 0.]*$/).replaceAll("")

        "fundamentals[$field] = ${actualValue}".shouldBe  "fundamentals[$field] = ${expectedValue}"
      }
    }
  }
  and "no alerts should have been issued", {
    def count = client.parserDaemonServiceBundle.alertService.alertsAsStrings.size()
    ensure(count){ isEqualTo 0 }
  }
}

// FIXME probably need some functionality for a timestamp message even when streaming not supported


//scenario "receiving a timestamp message", {
//  when "recieving a timestamp message"
//}
// T,20100513 20:42:00

//scenario "receiving multiple messages at the same time", {
//  when "receiving multiple messages with trade quotes in between as a single string", {
//    client.onAdminData([
//      "S,CURRENT UPDATE FIELDNAMES,Symbol,Last,Total Volume,Incremental Volume,Bid,Ask,Bid Size,Ask Size,Last Trade Time,Delay",
//      "S,UPDATE FIELDNAMES,Symbol,Exchange ID,Last,Change,Percent Change,Total Volume,Incremental Volume,High,Low,Bid,Ask,Bid Size,Ask Size,Tick,Bid Tick,Range,Last Trade Time,Open Interest,Open,Close,Spread,Strike,Settle,Delay,Market Center,Restricted Code,Net Asset Value,Average Maturity,7 Day Yield,Last Trade Date,(Reserved),Extended Trading Last,Expiration Date,Regional Volume,Net Asset Value 2,Extended Trading Change,Extended Trading Difference,Price-Earnings Ratio,Percent Off Average Volume,Bid Change,Ask Change,Change From Open,Market Open,Volatility,Market Capitalization,Fraction Display Code,Decimal Precision,Days to Expiration,Previous Day Volume,Regions,Open Range 1,Close Range 1,Open Range 2,Close Range 2,Number of Trades Today,Bid Time,Ask Time,VWAP,TickID,Financial Status Indicator,Settlement Date,Trade Market Center,Bid Market Center,Ask Market Center,Trade Time,Available Regions",
//      "S,FUNDAMENTAL FIELDNAMES,Symbol,Exchange ID,PE,Average Volume,52 Week High,52 Week Low,Calendar Year High,Calendar Year Low,Dividend Yield,Dividend Amount,Dividend Rate,Pay Date,Ex-dividend Date,(Reserved),(Reserved),(Reserved),Short Interest,(Reserved),Current Year EPS,Next Year EPS,Five-year Growth Percentage,Fiscal Year End,(Reserved),Company Name,Root Option Symbol,Percent Held By Institutions,Beta,Leaps,Current Assets,Current Liabilities,Balance Sheet Date,Long-term Debt,Common Shares Outstanding,(Reserved),Split Factor 1,Split Factor 2,(Reserved),Market Center,Format Code,Precision,SIC,Historical Volatility,Security Type,Listed Market,52 Week High Date,52 Week Low Date,Calendar Year High Date,Calendar Year Low Date,Year End Close,Maturity Date,Coupon Rate,Expiration Date,Strike Proce,NAICS"
//    ].join("\n"))
//    client.onLevelOneData([
//      "T,20100517 12:21:00",
//      "F,IBM,D,12.9,8620000,134.2500,99.5000,134.2500,116.0000,2.00,0.55,2.60,06/10/2010,05/06/2010,1,7,,9441017,,10.28,11.26,16.89,12,,INTERNATIONAL BUSINESS MACHINE,IBM,64.,0.69,VIB WIB XBY,48935.0,36002.0,03/01/2010,21932.0,1282348,334111,0.50 05/27/1999,0.50 05/28/1997,,n,14,4,3571,22.76,1,7,01/19/2010,07/08/2009,01/19/2010,05/06/2010,130.90,,,,,334111,,",
//      "F,AMD,D,6.1,32579000,10.2400,3.2200,10.2400,6.9800,,,,,,1,7,,41479267,,1.45,0.48,,12,,ADVANCED MICRO DEVICES,AMD,51.,2.33,,4275.0,2210.0,03/01/2010,4252.0,673365,334413,0.50 08/22/2000,0.50 08/23/1983,,p,14,4,3674,53.35,1,7,04/15/2010,07/08/2009,04/15/2010,02/05/2010,9.68,,,,,334413,,",
//      "Q,INTC,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:24t,,",
//      "P,INTC,F,21.6000,-0.29,-0.013248058,35072914,100,22.0200,21.4100,21.5900,21.6000,25400,14200,,,0.61,12:21:04t,,21.7500,21.8900,0.01,,,,0,,,,,05/17/2010,,21.6000,,,,-0.29,0.,19.816513761,-0.591200956,,,-0.15,0,0.028240741,120182400.00000001,14,4,,83526108,BSE-CSE-CHX-NYSE-PSE-NMS,,,,,102055,,,21.7698,774585,N,,5,11,5,12:21:04,5-8-10-11-12-13-16-18,",
//      "Q,AMD,8.6600,14280541,100,8.6500,8.6600,10900,5000,12:21:31t,,"
//    ].join("\n"))
//
//    waitForQueueToBeEmpty(client)
//  }
//  then "no alerts should have been issued",{
//    def alerts = client.parserDaemonServiceBundle.alertService.alertsAsStrings.findAll{ !it.toString().contains("[IQFeed latency]")}
//    def count = alerts.size()
//    ensure(count){ isEqualTo 0 }
//    // No longer sending timestamp message alerts. Info now in monitoring tab.
//  }
//  and "hasNext() should be true", {
//    client.hasNext().shouldBe true
//  }
//  and "next() should retrieve the INTC tick data", {
//    IRealTimeTradeRecord rec = client.next()
//    rec.symbol.shouldBe "INTC"
//    ensureFloatClose(rec.price, 21.6, 0.001)
//    rec.quantity.shouldBe 300
//    rec.internalTimestamp.shouldBeCloseTo(((12*60+21)*60+24)*1000,3)
//    // not testing isStale here
//  }
//  and "next() again should retrieve the AMD tick data", {
//    IRealTimeTradeRecord rec = client.next()
//    rec.symbol.shouldBe "AMD"
//    ensureFloatClose(rec.price, 8.66, 0.001)
//    rec.quantity.shouldBe 100
//    rec.internalTimestamp.shouldBeCloseTo(((12*60+21)*60+31)*1000,3)
//    // not testing isStale here
//  }
//  and "the global counter trade ticks counter should be 1", {
//    client.tradeTickCounter.shouldBe 2
//  }
//  and "the global counter non-trade ticks counter should be 0", {
//    client.nonTradeTickCounter.shouldBe 0
//  }
//}

//====================================================================================================================
// IRealTimeDataSource behavior
/*
scenario "responding to blockingWaitForData()", {
  given "a test coordinator", {
    coord = new TestCoordinator()
  }
  and "a release marker", {
    marker = "blocked" // remains blocked until unblocked
  }
  when "a main loop calls blockingWaitForData()", {
    mainLoop = {
      client.blockingWaitForData()
      marker = "unblocked" // mark that we got unblocked
      coord.finishTest()
    }
    new Thread(mainLoop).start()
  }
  then "the client is blocked", {
    sleep(100)
    marker.shouldBe "blocked"
  }
  and "it gets unblocked when new tick data is made available", {
    client.onAdminData("S,CURRENT UPDATE FIELDNAMES,Symbol,Last,Total Volume,Incremental Volume,Bid,Ask,Bid Size,Ask Size,Last Trade Time,Delay")
    client.onLevelOneData("Q,INTC,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:24t,,")
    waitForQueueToBeEmpty(client,10)
    coord.delayTestFinish(100)
    marker.shouldBe "unblocked"
  }
}


**************************************************************************************************************
other
scenario "hiccup detection", {
  given "that the hiccup alert threahold is 90msec", {
    client.maxMillisBetweenTicks = 90
  }
  and "TimeHelper in simulated mode", {
    mockTime = new MockTime(hour:11, increment:100, resetTimeHelper:true)
  }
  and "that at least one tick was already observed", {
    client.onAdminData("S,CURRENT UPDATE FIELDNAMES,Symbol,Last,Total Volume,Incremental Volume,Bid,Ask,Bid Size,Ask Size,Last Trade Time,Delay")
    client.onLevelOneData("Q,INTC,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:24t,,")
    waitForQueueToBeEmpty(client,10)
  }
  when "no ticks are observed in 300msec", {
    // sleep has to be long enough. Heartbeat is 100msec
    sleep(300)
  }
  then "a IQFeed hiccup alert should have been issued", {
    def alerts = client.heartBeatServicesBundle.alertService.alertsAsStrings
    ensure(alerts.size()) {
      isNotEqualTo 0
    }
    ensure(alerts[0]) {
      isNotNull
      and
      contains("[IQFeed hiccup]")
    }
  }
}

IMPORTANT: Stale ticks are tracked in the main loop and in the consumers. Here we just track the flow and order, not the age.

scenario "tracking out of order trade ticks", {
  when "we receive multiple ticks, but they come out of order", {
    client.onAdminData("S,CURRENT UPDATE FIELDNAMES,Symbol,Last,Total Volume,Incremental Volume,Bid,Ask,Bid Size,Ask Size,Last Trade Time,Delay")
    client.onLevelOneData("Q,INTC,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:24t,,") //OK
    client.onLevelOneData("Q,IBM,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:25t,,")  //OK
    client.onLevelOneData("Q,INTC,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:21t,,") // out of order
    client.onLevelOneData("Q,AMD,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:24t,,")  // out of order
    client.onLevelOneData("Q,INTC,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:25t,,") //OK
    waitForQueueToBeEmpty(client,10)
  }
  then "the outOfOrderTicks counter should reflect the number of ticks out of order", {
    ensure(client.outOfOrderTickCounter) {
      equals 2
    }
  }
}

// FIXME broken scenario
scenario "exposing stats updated via the heartbeat (queue lengths, counters, etc)", {
  given "that a few ticks were observed", {
    client.onAdminData("S,CURRENT UPDATE FIELDNAMES,Symbol,Last,Total Volume,Incremental Volume,Bid,Ask,Bid Size,Ask Size,Last Trade Time,Delay")
    client.onLevelOneData("Q,INTC,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:24t,,") //OK
    client.onLevelOneData("Q,IBM,21.6000,35089385,300,21.6000,21.6100,24800,10100,12:21:25t,,")  //OK
    waitForQueueToBeEmpty(client,10)
  }
  and "the stats object for the heartbeat thread", {
    stats = client.heartBeatServicesBundle.statsService
  }
  when "we wait a bit to have at least one heartbeat", {
    sleep(150)
  }
  [
    "rawMessageQueueSize",
    "tickQueueSize",
    "blockingWaitCounter",
    "tradeTickCounter",
    "nonTradeTickCounter",
    "lastObservedTickTs",
    "maxObservedTickTimestamp",
    "currentLocalBandwidthConsumption",
    "currentInternetBandwidthConsumption"
  ].each {stat ->
    then "the heartbeat stats should contain a value for $stat", {
      def statName = "IQFeed client $stat"
      ensure(stats.getStat(statName)) {
        isNotNull
      }
    }
  }
  [
    "staleTickCounter",
    "staleTickLastAge",
    "staleTickAverageAge",
    "staleTickMaxAge",
    "staleTickMinAge",
    "staleTickStdevAge",
    "outOfOrderTickCounter",
    "outOfOrderTicksCurried",
    "outOfOrderTickAverageAge",
    "outOfOrderTickMaxAge",
    "outOfOrderTickMinAge",
    "outOfOrderTickStdevAge",
  ].each {stat ->
    then "the heartbeat staleness stats should contain a value for $stat", {
      def statName = "IQFeed client staleness $stat"
      ensure(stats.getStat(statName)) {
        isNotNull
      }
    }
  }
}

*/
//====================================================================================================================
scenario "interacting with the facade with onSystemMessage", {
  when "the client gets a 'S,REGISTER CLIENT APP COMPLETED' message", {
    client.onAdminData("S,REGISTER CLIENT APP COMPLETED,")
  }
  then "the facade should receive a onSystemMessage() call", {
    ensureDoesNotThrow(Exception) {
      Mockito.verify(facade,Mockito.times(1)).onSystemMessage(["Q","REGISTER CLIENT APP COMPLETED"] as String[])
    }
  }
}

scenario "interacting with the facade with onSystemMessage (unknown message)", {
  when "the client gets a 'S,some random message' message", {
    client.onAdminData("S,some random message,")
  }
  then "the facade should receive a onSystemMessage() call", {
    ensureDoesNotThrow(Exception) {
      Mockito.verify(facade,Mockito.times(1)).onSystemMessage(["Q","REGISTER CLIENT APP COMPLETED"] as String[])
    }
  }
}

scenario "shutting down when onFatalException() received from facade", {
  when "the client receives an onFatalException", {
    client.onFatalException(new Exception("boom!"))
  }
  then "the client should shut down", {
    client.isShutdown().shouldBe true
    client.isConnected().shouldBe false
  }
}


// PASSIVE mode ======================================================================================================

scenario "initialization in passive mode", {
  given "a fresh facade", {
    // need a separate facade so we don't get double method invocation counting on it
    facade2 = Mockito.mock(IDtnIQFeedFacade)
    config.componentConfig["DtnIqfeedHistoricalClient"]["IDtnIQFeedFacade"] = facade2
    config.componentConfig["DtnIqfeedHistoricalClient"]["mode"] = "passive"
  }
  when "I initialize the client with a configuration", {
    client = new DtnIqfeedHistoricalClient()
    mockImmediateOnConnectionEstablished(facade2, client)
    shutdownList << client
  }
  then "initializing it would not cause exceptions", {
    ensureDoesNotThrow(Exception) {
      client.init(config)
    }
  }
  and "the facade should also get initialized with the process", {
    Mockito.verify(facade2, Mockito.times(1)).init(config)
  }
  and "the facade should be associated with the client", {
    Mockito.verify(facade2, Mockito.times(1)).setClient(client)
  }
  and "the facade should be instructed to initialize the connection in passive mode", {
    Mockito.verify(facade2, Mockito.times(1)).startIQConnectPassive()
  }
}

// As daily data source ==============================================================================================


scenario "client getting a missing symbol error", {
  given "a pre-defined response from the facade for sendGetDailyDataRequest", {
    response = "31 day BIDU,E,Invalid symbol.,\n31 day BIDU,!ENDMSG!,\n"
    result = null
    facade = new MockIQFeedFacade()
    client = createClient(facade,config)
    client.debugTag="debug missing symbol"
    //AtomicInteger
    facade.onDailyRequestResponse = {
      client.onLookupData(response)
    }
    facade.onFundamentalsRequestResponse = {
      client.onAdminData("S,FUNDAMENTAL FIELDNAMES,Symbol,Exchange ID,PE,Average Volume,52 Week High,52 Week Low,Calendar Year High,Calendar Year Low,Dividend Yield,Dividend Amount,Dividend Rate,Pay Date,Ex-dividend Date,(Reserved),(Reserved),(Reserved),Short Interest,(Reserved),Current Year EPS,Next Year EPS,Five-year Growth Percentage,Fiscal Year End,(Reserved),Company Name,Root Option Symbol,Percent Held By Institutions,Beta,Leaps,Current Assets,Current Liabilities,Balance Sheet Date,Long-term Debt,Common Shares Outstanding,(Reserved),Split Factor 1,Split Factor 2,(Reserved),Market Center,Format Code,Precision,SIC,Historical Volatility,Security Type,Listed Market,52 Week High Date,52 Week Low Date,Calendar Year High Date,Calendar Year Low Date,Year End Close,Maturity Date,Coupon Rate,Expiration Date,Strike Proce,NAICS")
      client.onLevelOneData("F,BIDU,F,61.0,16331000,82.2900,26.5540,82.2900,38.4661,,,,,,1,1,,13012241,,1.20,1.22,154.44,12,,BAIDU ADR,BIDU,60.,1.84,,709.4,205.0,03/01/2010,0.6,347850,517919,0.10 05/12/2010, ,,t,14,4,7375,70.51,1,1,05/13/2010,06/23/2009,05/13/2010,01/12/2010,41.12,,,,,517919,,")
    }

  }
  when "I call getMarketTreeDataFor on the client for BIDU", {
    result = [:]

    def requestThread = {
      try {
        item = client.getMarketTreeDataFor("BIDU", 600, TimeUnit.MILLISECONDS)
        result.prevClose = item.prevClose
        result.yearHigh = item.yearHigh
        result.yearLow = item.yearLow
        result.marketCap = item.marketCap
        result.averageDailyVolume = item.averageDailyVolume
      } catch (Exception e) {
        result.exception = e
      }

      //coord.finishTest()
    }
    //new Thread(requestThread).start()
    requestThread.call() // no thread
  }
  and "after the facade gets the fundamentals data", {
    //coord.delayTestFinish(100)
    //client.onAdminData("S,FUNDAMENTAL FIELDNAMES,Symbol,Exchange ID,PE,Average Volume,52 Week High,52 Week Low,Calendar Year High,Calendar Year Low,Dividend Yield,Dividend Amount,Dividend Rate,Pay Date,Ex-dividend Date,(Reserved),(Reserved),(Reserved),Short Interest,(Reserved),Current Year EPS,Next Year EPS,Five-year Growth Percentage,Fiscal Year End,(Reserved),Company Name,Root Option Symbol,Percent Held By Institutions,Beta,Leaps,Current Assets,Current Liabilities,Balance Sheet Date,Long-term Debt,Common Shares Outstanding,(Reserved),Split Factor 1,Split Factor 2,(Reserved),Market Center,Format Code,Precision,SIC,Historical Volatility,Security Type,Listed Market,52 Week High Date,52 Week Low Date,Calendar Year High Date,Calendar Year Low Date,Year End Close,Maturity Date,Coupon Rate,Expiration Date,Strike Proce,NAICS")
    //client.onLevelOneData("F,BIDU,F,61.0,16331000,82.2900,26.5540,82.2900,38.4661,,,,,,1,1,,13012241,,1.20,1.22,154.44,12,,BAIDU ADR,BIDU,60.,1.84,,709.4,205.0,03/01/2010,0.6,347850,517919,0.10 05/12/2010, ,,t,14,4,7375,70.51,1,1,05/13/2010,06/23/2009,05/13/2010,01/12/2010,41.12,,,,,517919,,")
  }
  and "after the facade calls the onLookupData() on we should see an exception", {
    //client.onLookupData(response)
    sleep(100)
    ensure(result.exception.getClass().name){isEqualTo("com.moscona.exceptions.InvalidStateException")}
    cause = MissingSymbolException.fishOutOf(result.exception,10)
    ensure(cause) {
      isNotNull()
    }
    ensure(cause.symbol){ isEqualTo "BIDU"}
    ensure(cause.literalError){ isEqualTo "Invalid symbol."}
  }
}



scenario "getting last close from facade", {
  given "a pre-defined response from the facade for sendGetDailyDataRequest", {
    response = biduResponse
    result = null
    facade = new MockIQFeedFacade()
    client = createClient(facade,config)
    //client.debugTag="debug"
    //AtomicInteger
    facade.onDailyRequestResponse = {
      client.onLookupData(biduResponse)
    }
    facade.onFundamentalsRequestResponse = {
      client.onAdminData("S,FUNDAMENTAL FIELDNAMES,Symbol,Exchange ID,PE,Average Volume,52 Week High,52 Week Low,Calendar Year High,Calendar Year Low,Dividend Yield,Dividend Amount,Dividend Rate,Pay Date,Ex-dividend Date,(Reserved),(Reserved),(Reserved),Short Interest,(Reserved),Current Year EPS,Next Year EPS,Five-year Growth Percentage,Fiscal Year End,(Reserved),Company Name,Root Option Symbol,Percent Held By Institutions,Beta,Leaps,Current Assets,Current Liabilities,Balance Sheet Date,Long-term Debt,Common Shares Outstanding,(Reserved),Split Factor 1,Split Factor 2,(Reserved),Market Center,Format Code,Precision,SIC,Historical Volatility,Security Type,Listed Market,52 Week High Date,52 Week Low Date,Calendar Year High Date,Calendar Year Low Date,Year End Close,Maturity Date,Coupon Rate,Expiration Date,Strike Proce,NAICS")
      client.onLevelOneData("F,BIDU,F,61.0,16331000,82.2900,26.5540,82.2900,38.4661,,,,,,1,1,,13012241,,1.20,1.22,154.44,12,,BAIDU ADR,BIDU,60.,1.84,,709.4,205.0,03/01/2010,0.6,347850,517919,0.10 05/12/2010, ,,t,14,4,7375,70.51,1,1,05/13/2010,06/23/2009,05/13/2010,01/12/2010,41.12,,,,,517919,,")
    }
  }
  when "I call getMarketTreeDataFor on the client for BIDU", {
    final returnedValue = [:]
    result = returnedValue
    def requestThread = {
      item = client.getMarketTreeDataFor("BIDU", 600, TimeUnit.MILLISECONDS)
      returnedValue.prevClose = item.prevClose
      returnedValue.yearHigh = item.yearHigh
      returnedValue.yearLow = item.yearLow
      returnedValue.marketCap = item.marketCap
      returnedValue.averageDailyVolume = item.averageDailyVolume
      //coord.finishTest()
    }
    //new Thread(requestThread).start()
    printExceptions {
      requestThread.call() // no background thread at all...
    }
  }
  and "after the facade gets the fundamentals data", {
    sleep(1000)
    //coord.delayTestFinish(100)
    //waitForQueueToBeEmpty(client,100)
    while (client.rawMessageQueueSize>0){
      "...$client.debugTag waiting..."
      sleep(100)
    }
    //client.onAdminData("S,FUNDAMENTAL FIELDNAMES,Symbol,Exchange ID,PE,Average Volume,52 Week High,52 Week Low,Calendar Year High,Calendar Year Low,Dividend Yield,Dividend Amount,Dividend Rate,Pay Date,Ex-dividend Date,(Reserved),(Reserved),(Reserved),Short Interest,(Reserved),Current Year EPS,Next Year EPS,Five-year Growth Percentage,Fiscal Year End,(Reserved),Company Name,Root Option Symbol,Percent Held By Institutions,Beta,Leaps,Current Assets,Current Liabilities,Balance Sheet Date,Long-term Debt,Common Shares Outstanding,(Reserved),Split Factor 1,Split Factor 2,(Reserved),Market Center,Format Code,Precision,SIC,Historical Volatility,Security Type,Listed Market,52 Week High Date,52 Week Low Date,Calendar Year High Date,Calendar Year Low Date,Year End Close,Maturity Date,Coupon Rate,Expiration Date,Strike Proce,NAICS")
    //client.onLevelOneData("F,BIDU,F,61.0,16331000,82.2900,26.5540,82.2900,38.4661,,,,,,1,1,,13012241,,1.20,1.22,154.44,12,,BAIDU ADR,BIDU,60.,1.84,,709.4,205.0,03/01/2010,0.6,347850,517919,0.10 05/12/2010, ,,t,14,4,7375,70.51,1,1,05/13/2010,06/23/2009,05/13/2010,01/12/2010,41.12,,,,,517919,,")
  }
  then "the facade should get a sendGetDailyDataRequest", {
    facade.sendGetDailyDataRequestCalls[["31 day BIDU","BIDU",31]].shouldBe 1
  }
  then "the facade should get a sendGetFundamentalsRequest", {
    facade.sendGetFundamentalsRequestCalls["BIDU"].shouldBeGreaterThan 0
  }
  and """after the facade calls the onLookupData() on the client with the response the client should return the latest market tree entry""", {
    //client.onLookupData(response)
    sleep(100)
    ensureFloatClose(result.prevClose, 69.81, 0.005)
    result.averageDailyVolume.shouldBe 16331000 // this now comes from fundamentals
    ensureFloatClose(result.yearHigh, 82.29, 0.005)
    ensureFloatClose(result.yearLow, 26.55, 0.005)
    ensureFloatClose(result.marketCap, 24283408.5, 1.0)
  }
}

scenario "getting minute bars", {
  // SymbolChart getMinuteBars(String symbol, Calendar from, Calendar to, int timeout, TimeUnit unit)
  given "start and end times", {
    from = TimeHelper.parseTimeOfDayExpression("09:30:00","09:30:00")
    to = TimeHelper.parseTimeOfDayExpression("09:40:10","09:40:10")
  }
  and "a pre-defined response from the facade for sendMinuteDataRequest", {

    response = """__REQUEST_ID__,__DATE__ 09:31:00,74.2299,69.2300,74.0400,69.8100,10261529,1234,
__REQUEST_ID__,__DATE__ 09:32:00,76.2390,73.1500,75.6000,73.2000,9173737,1234,
__REQUEST_ID__,__DATE__ 09:33:00,77.9700,74.9200,77.3200,76.6640,14230112,1234,
__REQUEST_ID__,__DATE__ 09:34:00,76.0000,73.3700,74.6600,75.9800,9430675,1234,
__REQUEST_ID__,__DATE__ 09:35:00,75.6000,72.1300,72.8450,73.5600,11880727,1234,
__REQUEST_ID__,__DATE__ 09:36:00,74.2000,72.1200,74.2000,73.2100,10417050,1234,
__REQUEST_ID__,__DATE__ 09:37:00,73.5100,69.5600,70.4900,73.4999,14046857,1234,
__REQUEST_ID__,__DATE__ 09:38:00,72.1000,67.5700,71.7600,67.5900,12467576,1234,
__REQUEST_ID__,__DATE__ 09:39:00,69.4550,66.4301,67.1800,69.0800,15888915,1234,
__REQUEST_ID__,__DATE__ 09:40:00,73.3500,70.9400,72.4500,71.0051,10665417,1234,
__REQUEST_ID__,!ENDMSG!,
""".replaceAll("__DATE__",TimeHelper.toDayString(from))

    result = null
    facade = new MockIQFeedFacade()
    client = createClient(facade,config)
    facade.client = client
    //client.debugTag="debug"
    //AtomicInteger
    facade.minuteDataResponse = response
  }
  when "I call getMinuteBars on the client for BIDU", {
    ensureDoesNotThrow(Exception) {
      printExceptions {
        result = client.getMinuteBars("BIDU", from, to, 100, TimeUnit.MILLISECONDS, 0)
      }
    }
  }
  then "the facade should have been called with the correct from argument", {
    facade.fromParam.shouldMatch(/(\d){8} 093000/)
  }
  then "the facade should have been called with the correct to argument", {
    facade.toParam.shouldMatch(/(\d){8} 093900/)
  }
  then "I should get ten bars", {
    ensure(result){isNotNull()}
    result.size().shouldBe(10)
  }
  and "the first bar should match the first response", {
    bar = result.getBar(0)
    "$bar.highCents $bar.lowCents $bar.openCents $bar.closeCents $bar.volume".shouldBe("7423 6923 7404 6981 1234")
  }
  and "the last bar should match the last response", {
    bar = result.getBar(9)
    "$bar.highCents $bar.lowCents $bar.openCents $bar.closeCents $bar.volume".shouldBe("7335 7094 7245 7101 1234")
  }
  and "the symbol should match", {
    result.symbol.shouldBe("BIDU")
  }
}

scenario "getting day bars", {
  // SymbolChart getMinuteBars(String symbol, Calendar from, Calendar to, int timeout, TimeUnit unit)
  given "start and end times", {
    from = TimeHelper.parseMmDdYyyyDate("07/31/2013")
    to = TimeHelper.parseMmDdYyyyDate("06/29/2014")
  }
  and "a pre-defined response from the facade for sendMinuteDataRequest", {

    response = """__REQUEST_ID__,2013-07-31 08:34:30,52.96,51.71,51.71,52.14,27867562,0,
__REQUEST_ID__,2013-08-01 08:34:30,53.20,52.74,52.84,52.86,20583956,0,
__REQUEST_ID__,2013-08-02 08:34:30,53.05,52.50,52.84,53.00,15440120,0,
__REQUEST_ID__,2013-08-05 08:34:30,53.11,52.75,52.93,52.87,10708413,0,
__REQUEST_ID__,2013-08-06 08:34:30,52.69,51.40,52.59,51.48,23701553,0,
__REQUEST_ID__,2013-08-07 08:34:30,51.64,50.80,51.01,51.50,18627169,0,
__REQUEST_ID__,2013-08-08 08:34:30,52.12,51.07,52.09,51.78,18630797,0,
__REQUEST_ID__,2013-08-09 08:34:30,52.00,51.24,51.72,51.32,15628880,0,
__REQUEST_ID__,2013-08-12 08:34:30,51.12,50.65,51.00,50.90,17413145,0,
__REQUEST_ID__,2013-08-13 08:34:30,52.15,50.74,51.04,51.77,23815593,0,
__REQUEST_ID__,2013-08-14 08:34:30,52.44,51.58,51.95,51.59,17240537,0,
__REQUEST_ID__,2013-08-15 08:34:30,51.10,50.33,51.02,50.86,26231374,0,
__REQUEST_ID__,2013-08-16 08:34:30,51.08,50.22,50.69,50.35,23497081,0,
__REQUEST_ID__,2013-08-19 08:34:30,50.13,49.30,50.06,49.33,22958756,0,
__REQUEST_ID__,2013-08-20 08:34:30,50.25,48.77,49.27,49.92,29313326,0,
__REQUEST_ID__,2013-08-21 08:34:30,49.94,49.03,49.80,49.16,30669552,0,
__REQUEST_ID__,2013-08-22 08:34:30,50.27,49.28,49.66,49.91,22411918,0,
__REQUEST_ID__,2013-08-23 08:34:30,50.33,49.69,50.11,49.83,15306297,0,
__REQUEST_ID__,2013-08-26 08:34:30,50.34,49.60,49.80,49.60,16604006,0,
__REQUEST_ID__,2013-08-27 08:34:30,49.34,48.16,48.84,48.24,29854882,0,
__REQUEST_ID__,2013-08-28 08:34:30,48.93,47.64,48.05,48.31,29623231,0,
__REQUEST_ID__,2013-08-29 08:34:30,48.90,48.27,48.40,48.47,17293719,0,
__REQUEST_ID__,2013-08-30 08:34:30,48.71,47.80,48.71,48.33,24908202,0,
__REQUEST_ID__,2013-09-03 08:34:30,49.84,48.36,49.37,49.37,28909136,0,
__REQUEST_ID__,2013-09-04 08:34:30,49.95,49.12,49.31,49.60,18961346,0,
__REQUEST_ID__,2013-09-05 08:34:30,50.200,49.622,49.850,49.860,17765782,0,
__REQUEST_ID__,2013-09-06 08:34:30,50.18,48.89,50.18,49.22,24627143,0,
__REQUEST_ID__,2013-09-09 08:34:30,50.12,49.36,49.49,50.09,22391439,0,
__REQUEST_ID__,2013-09-10 08:34:30,51.53,50.54,50.74,51.09,24844652,0,
__REQUEST_ID__,2013-09-11 08:34:30,51.12,50.26,51.04,50.73,22263288,0,
__REQUEST_ID__,2013-09-12 08:34:30,50.7600,49.9733,50.6600,50.2600,21345607,0,
__REQUEST_ID__,2013-09-13 08:34:30,50.62,50.22,50.29,50.49,12576046,0,
__REQUEST_ID__,2013-09-16 08:34:30,51.45,50.74,51.19,51.00,19398477,0,
__REQUEST_ID__,2013-09-17 08:34:30,51.28,50.87,51.15,51.20,15468925,0,
__REQUEST_ID__,2013-09-18 08:34:30,52.66,50.98,51.02,52.21,31919519,0,
__REQUEST_ID__,2013-09-19 08:34:30,52.88,51.90,52.75,51.95,25377042,0,
__REQUEST_ID__,2013-09-20 08:34:30,52.10,51.12,52.03,51.21,28174643,0,
__REQUEST_ID__,2013-09-23 08:34:30,50.1700,49.2900,49.9499,49.5700,34571718,0,
__REQUEST_ID__,2013-09-24 08:34:30,49.48,48.93,49.43,48.96,26564582,0,
__REQUEST_ID__,2013-09-25 08:34:30,49.54,48.67,49.04,49.26,26934629,0,
__REQUEST_ID__,2013-09-26 08:34:30,49.49,48.50,49.27,48.93,23345377,0,
__REQUEST_ID__,2013-09-27 08:34:30,49.20,48.69,48.76,48.89,20248714,0,
__REQUEST_ID__,2013-09-30 08:34:30,48.86,47.83,48.27,48.51,23797198,0,
__REQUEST_ID__,2013-10-01 08:34:30,49.10,48.42,48.85,48.60,21040399,0,
__REQUEST_ID__,2013-10-02 08:34:30,49.0499,48.1600,48.4200,48.7100,23568301,0,
__REQUEST_ID__,2013-10-03 08:34:30,48.9800,48.0296,48.8100,48.4000,22018731,0,
__REQUEST_ID__,2013-10-04 08:34:30,49.19,48.38,48.45,49.14,18435987,0,
__REQUEST_ID__,2013-10-07 08:34:30,48.67,48.16,48.62,48.18,19144316,0,
__REQUEST_ID__,2013-10-08 08:34:30,48.45,47.62,48.22,47.67,24655341,0,
__REQUEST_ID__,2013-10-09 08:34:30,48.38,47.60,47.98,47.95,22607104,0,
__REQUEST_ID__,2013-10-10 08:34:30,49.365,48.570,48.800,49.270,23989575,0,
__REQUEST_ID__,2013-10-11 08:34:30,49.36,48.70,49.35,49.22,22990034,0,
__REQUEST_ID__,2013-10-14 08:34:30,49.90,48.55,48.83,49.60,24765942,0,
__REQUEST_ID__,2013-10-15 08:34:30,49.99,48.65,49.69,48.86,49035627,0,
__REQUEST_ID__,2013-10-16 08:34:30,50.90,49.27,49.43,50.84,41573989,0,
__REQUEST_ID__,2013-10-17 08:34:30,51.22,50.29,50.38,51.12,28304595,0,
__REQUEST_ID__,2013-10-18 08:34:30,51.37,50.81,51.37,51.15,24570317,0,
__REQUEST_ID__,2013-10-21 08:34:30,51.57,51.00,51.19,51.03,18703075,0,
__REQUEST_ID__,2013-10-22 08:34:30,51.39,50.58,51.32,50.76,27820372,0,
__REQUEST_ID__,2013-10-23 08:34:30,50.53,49.92,50.47,50.19,19530758,0,
__REQUEST_ID__,2013-10-24 08:34:30,50.31,49.77,50.27,50.15,20044530,0,
__REQUEST_ID__,2013-10-25 08:34:30,50.26,49.95,50.03,50.06,16736563,0,
__REQUEST_ID__,2013-10-28 08:34:30,50.33,49.96,50.01,50.15,15543315,0,
__REQUEST_ID__,2013-10-29 08:34:30,50.46,50.07,50.27,50.22,16418523,0,
__REQUEST_ID__,2013-10-30 08:34:30,50.50,49.59,50.50,49.89,22438683,0,
__REQUEST_ID__,2013-10-31 08:34:30,49.74,48.77,49.74,48.78,25056880,0,
__REQUEST_ID__,2013-11-01 08:34:30,49.02,48.61,48.87,48.74,19004072,0,
__REQUEST_ID__,2013-11-04 08:34:30,49.14,48.58,48.84,48.63,16515710,0,
__REQUEST_ID__,2013-11-05 08:34:30,48.62,48.23,48.50,48.38,18402671,0,
__REQUEST_ID__,2013-11-06 08:34:30,48.82,48.39,48.76,48.62,17621520,0,
__REQUEST_ID__,2013-11-07 08:34:30,49.13,48.35,48.93,48.35,26957278,0,
__REQUEST_ID__,2013-11-08 08:34:30,50.17,48.47,48.47,49.94,35134406,0,
__REQUEST_ID__,2013-11-11 08:34:30,50.30,49.36,49.79,50.17,16839483,0,
__REQUEST_ID__,2013-11-12 08:34:30,50.01,49.35,50.00,49.52,21630731,0,
__REQUEST_ID__,2013-11-13 08:34:30,49.99,49.02,49.28,49.99,23536421,0,
__REQUEST_ID__,2013-11-14 08:34:30,50.29,49.55,50.01,50.21,21758289,0,
__REQUEST_ID__,2013-11-15 08:34:30,50.78,50.04,50.19,50.40,22218640,0,
__REQUEST_ID__,2013-11-18 08:34:30,51.40,50.57,50.80,50.79,26353367,0,
__REQUEST_ID__,2013-11-19 08:34:30,51.44,50.61,50.76,51.17,20113270,0,
__REQUEST_ID__,2013-11-20 08:34:30,51.29,50.62,51.05,50.77,18351229,0,
__REQUEST_ID__,2013-11-21 08:34:30,51.97,50.78,50.86,51.73,22868922,0,
__REQUEST_ID__,2013-11-22 08:34:30,52.54,51.84,51.94,52.41,26055018,0,
__REQUEST_ID__,2013-11-25 08:34:30,53.68,52.48,52.49,53.29,32610039,0,
__REQUEST_ID__,2013-11-26 08:34:30,53.46,52.96,53.10,53.01,17667414,0,
__REQUEST_ID__,2013-11-27 08:34:30,53.26,52.90,52.97,53.05,13075242,0,
__REQUEST_ID__,2013-11-29 08:34:30,53.59,52.86,53.11,52.92,10952127,0,
__REQUEST_ID__,2013-12-02 08:34:30,53.41,52.53,52.96,52.62,19854946,0,
__REQUEST_ID__,2013-12-03 08:34:30,52.76,51.73,52.47,52.13,20597447,0,
__REQUEST_ID__,2013-12-04 08:34:30,52.31,51.05,51.14,52.04,32155330,0,
__REQUEST_ID__,2013-12-05 08:34:30,51.80,50.95,51.61,51.06,23556852,0,
__REQUEST_ID__,2013-12-06 08:34:30,51.99,51.20,51.74,51.49,19718733,0,
__REQUEST_ID__,2013-12-09 08:34:30,52.46,51.92,51.94,52.11,16653486,0,
__REQUEST_ID__,2013-12-10 08:34:30,52.610,51.685,51.950,51.740,20025706,0,
__REQUEST_ID__,2013-12-11 08:34:30,51.65,50.53,51.62,50.71,24599973,0,
__REQUEST_ID__,2013-12-12 08:34:30,51.14,50.27,51.00,50.91,27240785,0,
__REQUEST_ID__,2013-12-13 08:34:30,51.19,50.68,50.98,50.97,16753273,0,
__REQUEST_ID__,2013-12-16 08:34:30,51.61,50.82,51.39,50.90,20319461,0,
__REQUEST_ID__,2013-12-17 08:34:30,51.05,50.55,51.00,50.69,17951722,0,
__REQUEST_ID__,2013-12-18 08:34:30,52.07,50.33,50.81,51.96,32921338,0,
__REQUEST_ID__,2013-12-19 08:34:30,52.13,51.45,51.73,51.88,20156250,0,
__REQUEST_ID__,2013-12-20 08:34:30,52.44,51.90,52.03,52.21,27878630,0,
__REQUEST_ID__,2013-12-23 08:34:30,52.64,52.22,52.49,52.41,13633839,0,
__REQUEST_ID__,2013-12-24 08:34:30,52.47,52.08,52.36,52.43,5302632,0,
__REQUEST_ID__,2013-12-26 08:34:30,52.69,52.20,52.62,52.35,8885384,0,
__REQUEST_ID__,2013-12-27 08:34:30,52.41,52.19,52.36,52.26,11816775,0,
__REQUEST_ID__,2013-12-30 08:34:30,52.44,51.83,52.25,51.92,11511778,0,
__REQUEST_ID__,2013-12-31 08:34:30,52.12,51.81,51.96,52.11,10714210,0,
__REQUEST_ID__,2014-01-02 08:34:30,52.40,51.81,52.03,52.27,16489767,0,
__REQUEST_ID__,2014-01-03 08:34:30,53.47,52.31,52.39,53.40,26900637,0,
__REQUEST_ID__,2014-01-06 08:34:30,54.29,53.43,53.62,53.81,28524005,0,
__REQUEST_ID__,2014-01-07 08:34:30,54.72,53.78,54.59,54.18,28855151,0,
__REQUEST_ID__,2014-01-08 08:34:30,55.00,54.20,54.29,54.81,26002326,0,
__REQUEST_ID__,2014-01-09 08:34:30,55.28,54.76,55.00,55.20,21812007,0,
__REQUEST_ID__,2014-01-10 08:34:30,55.0866,54.3000,55.0300,54.7200,22496888,0,
__REQUEST_ID__,2014-01-13 08:34:30,54.66,53.44,54.46,53.72,21116568,0,
__REQUEST_ID__,2014-01-14 08:34:30,54.40,53.63,54.13,53.95,17790818,0,
__REQUEST_ID__,2014-01-15 08:34:30,55.17,54.36,54.52,54.99,33721179,0,
__REQUEST_ID__,2014-01-16 08:34:30,53.56,52.35,53.56,52.60,62975999,0,
__REQUEST_ID__,2014-01-17 08:34:30,52.92,52.13,52.65,52.27,33944484,0,
__REQUEST_ID__,2014-01-21 08:34:30,53.00,51.56,52.50,51.85,27577062,0,
__REQUEST_ID__,2014-01-22 08:34:30,52.1250,51.5101,52.0100,51.9000,20851684,0,
__REQUEST_ID__,2014-01-23 08:34:30,51.65,50.55,51.65,50.72,34854131,0,
__REQUEST_ID__,2014-01-24 08:34:30,50.17,49.08,50.05,49.33,59204796,0,
__REQUEST_ID__,2014-01-27 08:34:30,49.66,48.36,49.10,48.81,39704016,0,
__REQUEST_ID__,2014-01-28 08:34:30,49.86,49.29,49.29,49.60,24815511,0,
__REQUEST_ID__,2014-01-29 08:34:30,49.36,47.70,48.92,48.08,54085368,0,
__REQUEST_ID__,2014-01-30 08:34:30,48.70,47.78,48.46,48.30,39952062,0,
__REQUEST_ID__,2014-01-31 08:34:30,48.20,47.29,47.51,47.43,33437552,0,
__REQUEST_ID__,2014-02-03 08:34:30,47.86,46.19,47.84,46.34,42995248,0,
__REQUEST_ID__,2014-02-04 08:34:30,47.50,46.68,47.00,46.78,31320159,0,
__REQUEST_ID__,2014-02-05 08:34:30,47.37,46.31,46.76,47.06,32540249,0,
__REQUEST_ID__,2014-02-06 08:34:30,48.27,47.27,47.40,48.25,33920886,0,
__REQUEST_ID__,2014-02-07 08:34:30,49.39,48.52,49.09,49.34,37935595,0,
__REQUEST_ID__,2014-02-10 08:34:30,49.73,48.83,49.55,49.32,19640119,0,
__REQUEST_ID__,2014-02-11 08:34:30,49.86,48.96,49.24,49.66,22992647,0,
__REQUEST_ID__,2014-02-12 08:34:30,50.145,49.650,49.780,49.960,23627925,0,
__REQUEST_ID__,2014-02-13 08:34:30,49.93,49.05,49.07,49.86,20656743,0,
__REQUEST_ID__,2014-02-14 08:34:30,49.82,49.41,49.57,49.52,18867147,0,
__REQUEST_ID__,2014-02-18 08:34:30,49.9056,49.3550,49.5400,49.3800,16223227,0,
__REQUEST_ID__,2014-02-19 08:34:30,49.31,48.19,49.01,48.19,30305903,0,
__REQUEST_ID__,2014-02-20 08:34:30,48.535,47.885,48.480,48.130,25242451,0,
__REQUEST_ID__,2014-02-21 08:34:30,48.50,48.10,48.22,48.26,26883446,0,
__REQUEST_ID__,2014-02-24 08:34:30,49.483,48.310,48.330,48.980,23880484,0,
__REQUEST_ID__,2014-02-25 08:34:30,49.06,48.30,48.94,48.40,20509553,0,
__REQUEST_ID__,2014-02-26 08:34:30,48.61,47.68,48.43,48.32,28825528,0,
__REQUEST_ID__,2014-02-27 08:34:30,48.69,47.96,48.09,48.69,18602465,0,
__REQUEST_ID__,2014-02-28 08:34:30,49.29,48.11,48.27,48.63,31735422,0,
__REQUEST_ID__,2014-03-03 08:34:30,48.25,47.55,47.80,47.61,28753674,0,
__REQUEST_ID__,2014-03-04 08:34:30,48.96,48.14,48.33,48.83,23956058,0,
__REQUEST_ID__,2014-03-05 08:34:30,49.9200,48.9599,49.0400,49.4200,23628360,0,
__REQUEST_ID__,2014-03-06 08:34:30,50.19,49.68,49.90,49.71,26975414,0,
__REQUEST_ID__,2014-03-07 08:34:30,50.41,49.37,50.21,49.62,25784426,0,
__REQUEST_ID__,2014-03-10 08:34:30,49.61,49.08,49.16,49.57,15398387,0,
__REQUEST_ID__,2014-03-11 08:34:30,49.82,48.40,49.78,48.43,33264192,0,
__REQUEST_ID__,2014-03-12 08:34:30,48.1400,47.7601,48.1200,47.9800,25861551,0,
__REQUEST_ID__,2014-03-13 08:34:30,48.295,47.070,48.170,47.330,38925373,0,
__REQUEST_ID__,2014-03-14 08:34:30,47.82,46.79,47.37,46.88,26135925,0,
__REQUEST_ID__,2014-03-17 08:34:30,47.80,47.31,47.47,47.73,18929683,0,
__REQUEST_ID__,2014-03-18 08:34:30,48.14,47.50,47.75,48.14,19517186,0,
__REQUEST_ID__,2014-03-19 08:34:30,49.5201,48.0200,48.2200,48.9400,30308826,0,
__REQUEST_ID__,2014-03-20 08:34:30,50.46,49.01,49.29,50.22,33885644,0,
__REQUEST_ID__,2014-03-21 08:34:30,51.00,49.86,51.00,50.08,38406929,0,
__REQUEST_ID__,2014-03-24 08:34:30,50.65,49.78,50.20,50.05,21170311,0,
__REQUEST_ID__,2014-03-25 08:34:30,50.56,50.05,50.21,50.30,17486616,0,
__REQUEST_ID__,2014-03-26 08:34:30,50.580,49.675,50.530,50.160,36784712,0,
__REQUEST_ID__,2014-03-27 08:34:30,48.20,47.11,47.43,47.45,112601033,0,
__REQUEST_ID__,2014-03-28 08:34:30,47.75,47.03,47.72,47.25,39796005,0,
__REQUEST_ID__,2014-03-31 08:34:30,47.8999,47.4700,47.5300,47.6000,25651026,0,
__REQUEST_ID__,2014-04-01 08:34:30,48.25,47.65,47.72,47.80,26813780,0,
__REQUEST_ID__,2014-04-02 08:34:30,48.36,47.73,48.15,48.24,22354287,0,
__REQUEST_ID__,2014-04-03 08:34:30,48.00,47.37,47.94,47.68,29237080,0,
__REQUEST_ID__,2014-04-04 08:34:30,47.79,46.85,47.73,47.11,33010005,0,
__REQUEST_ID__,2014-04-07 08:34:30,47.17,46.29,47.12,46.55,30154162,0,
__REQUEST_ID__,2014-04-08 08:34:30,46.85,46.12,46.53,46.60,26880730,0,
__REQUEST_ID__,2014-04-09 08:34:30,47.19,46.32,46.78,47.16,22380106,0,
__REQUEST_ID__,2014-04-10 08:34:30,47.27,46.12,47.24,46.23,27169989,0,
__REQUEST_ID__,2014-04-11 08:34:30,46.42,45.18,45.70,45.68,31063168,0,
__REQUEST_ID__,2014-04-14 08:34:30,47.80,46.98,47.40,47.67,51970279,0,
__REQUEST_ID__,2014-04-15 08:34:30,48.46,47.64,47.96,48.31,36511591,0,
__REQUEST_ID__,2014-04-16 08:34:30,48.62,47.58,48.56,48.18,24658051,0,
__REQUEST_ID__,2014-04-17 08:34:30,48.44,47.99,48.19,48.22,21361800,0,
__REQUEST_ID__,2014-04-21 08:34:30,48.22,47.77,48.18,47.84,17048870,0,
__REQUEST_ID__,2014-04-22 08:34:30,48.55,47.85,47.90,48.02,18587072,0,
__REQUEST_ID__,2014-04-23 08:34:30,48.50,47.91,48.00,48.40,13500809,0,
__REQUEST_ID__,2014-04-24 08:34:30,48.58,48.01,48.45,48.33,14699644,0,
__REQUEST_ID__,2014-04-25 08:34:30,48.423,47.680,48.120,47.750,18120477,0,
__REQUEST_ID__,2014-04-28 08:34:30,47.97,47.05,47.64,47.30,25780296,0,
__REQUEST_ID__,2014-04-29 08:34:30,48.20,47.40,47.60,48.16,16943495,0,
__REQUEST_ID__,2014-04-30 08:34:30,48.33,47.84,48.13,47.91,17079734,0,
__REQUEST_ID__,2014-05-01 08:34:30,48.14,47.59,47.81,47.76,10940267,0,
__REQUEST_ID__,2014-05-02 08:34:30,48.21,47.49,47.83,47.73,15362235,0,
__REQUEST_ID__,2014-05-05 08:34:30,47.31,46.80,47.20,47.18,16854666,0,
__REQUEST_ID__,2014-05-06 08:34:30,47.05,46.33,47.01,46.36,21786727,0,
__REQUEST_ID__,2014-05-07 08:34:30,46.83,46.32,46.54,46.70,17502520,0,
__REQUEST_ID__,2014-05-08 08:34:30,47.49,46.70,46.70,47.14,16067736,0,
__REQUEST_ID__,2014-05-09 08:34:30,47.23,46.71,47.04,46.99,15185788,0,
__REQUEST_ID__,2014-05-12 08:34:30,47.4000,47.0400,47.2201,47.2700,13308290,0,
__REQUEST_ID__,2014-05-13 08:34:30,47.49,47.23,47.35,47.42,12615169,0,
__REQUEST_ID__,2014-05-14 08:34:30,47.51,46.98,47.50,47.12,12406415,0,
__REQUEST_ID__,2014-05-15 08:34:30,46.94,46.15,46.85,46.52,21179067,0,
__REQUEST_ID__,2014-05-16 08:34:30,46.675,46.170,46.440,46.440,17188597,0,
__REQUEST_ID__,2014-05-19 08:34:30,46.79,46.08,46.10,46.77,13631001,0,
__REQUEST_ID__,2014-05-20 08:34:30,46.95,46.38,46.62,46.55,11792403,0,
__REQUEST_ID__,2014-05-21 08:34:30,47.09,46.69,46.83,46.85,12823793,0,
__REQUEST_ID__,2014-05-22 08:34:30,47.15,46.77,46.78,47.14,12713467,0,
__REQUEST_ID__,2014-05-23 08:34:30,47.40,47.09,47.23,47.29,9572844,0,
__REQUEST_ID__,2014-05-27 08:34:30,48.35,47.41,47.62,47.57,24507605,0,
__REQUEST_ID__,2014-05-28 08:34:30,47.89,47.31,47.48,47.32,14179968,0,
__REQUEST_ID__,2014-05-29 08:34:30,47.50,47.21,47.35,47.28,15554537,0,
__REQUEST_ID__,2014-05-30 08:34:30,47.60,47.15,47.19,47.57,11540295,0,
__REQUEST_ID__,2014-06-02 08:34:30,47.96,47.43,47.60,47.76,11890290,0,
__REQUEST_ID__,2014-06-03 08:34:30,48.26,47.46,47.51,48.19,16107418,0,
__REQUEST_ID__,2014-06-04 08:34:30,48.195,47.815,48.040,47.880,13077819,0,
__REQUEST_ID__,2014-06-05 08:34:30,48.80,47.89,48.01,48.63,17054195,0,
__REQUEST_ID__,2014-06-06 08:34:30,49.20,48.62,48.70,48.93,19151076,0,
__REQUEST_ID__,2014-06-09 08:34:30,49.59,48.95,48.99,49.58,14237461,0,
__REQUEST_ID__,2014-06-10 08:34:30,49.50,48.99,49.49,49.33,12771225,0,
__REQUEST_ID__,2014-06-11 08:34:30,49.270,48.635,49.030,48.810,13944243,0,
__REQUEST_ID__,2014-06-12 08:34:30,48.71,48.10,48.62,48.27,15672968,0,
__REQUEST_ID__,2014-06-13 08:34:30,48.67,47.12,48.14,47.59,34838669,0,
__REQUEST_ID__,2014-06-16 08:34:30,47.78,47.04,47.47,47.64,12791370,0,
__REQUEST_ID__,2014-06-17 08:34:30,48.05,47.35,47.48,47.79,13951109,0,
__REQUEST_ID__,2014-06-18 08:34:30,48.00,47.41,47.77,47.93,14472986,0,
__REQUEST_ID__,2014-06-19 08:34:30,47.9700,47.3375,47.9500,47.5600,15756633,0,
__REQUEST_ID__,2014-06-20 08:34:30,47.82,47.07,47.77,47.34,18833091,0,
__REQUEST_ID__,2014-06-23 08:34:30,48.175,47.100,47.130,48.060,18434132,0,
__REQUEST_ID__,2014-06-24 08:34:30,48.48,47.71,47.85,47.81,16920684,0,
__REQUEST_ID__,2014-06-25 08:34:30,48.070,47.555,47.670,47.820,12706655,0,
__REQUEST_ID__,2014-06-26 08:34:30,47.739,47.030,47.710,47.230,18878483,0,
__REQUEST_ID__,2014-06-27 08:34:30,47.34,46.92,47.22,47.14,19164205,0,
__REQUEST_ID__,!ENDMSG!,
"""

    result = null
    facade = new MockIQFeedFacade()
    client = createClient(facade,config)
    facade.client = client
    //client.debugTag="debug"
    //AtomicInteger
    facade.dayDataResponse = response
  }
  when "I call getDayBars on the client for BIDU", {
    ensureDoesNotThrow(Exception) {
      printExceptions {
        result = client.getDayBars("C", from, to, 100, TimeUnit.MILLISECONDS, 0)
      }
    }
  }
  then "the facade should have been called with the correct from argument", {
    facade.fromParam.shouldMatch(/,20130731,/)
  }
  then "the facade should have been called with the correct to argument", {
    facade.toParam.shouldMatch(/,20140629,/)
  }
  then "I should get 230 bars", {
    ensure(result){isNotNull()}
    result.size().shouldBe(230)
  }
  and "the first bar should match the first response", {
    bar = result.getBar(0)
    "$bar.highCents $bar.lowCents $bar.openCents $bar.closeCents $bar.volume".shouldBe("5296 5171 5171 5214 27867562")
  }
  and "the last bar should match the last response", {
    bar = result.getBar(229)
    "$bar.highCents $bar.lowCents $bar.openCents $bar.closeCents $bar.volume".shouldBe("4734 4692 4722 4714 19164205")
  }
  and "the symbol should match", {
    result.symbol.shouldBe("C")
  }
}

scenario "getting seconds bars", {
  given "start and end times", {
    from = TimeHelper.parseTimeOfDayExpression("09:40:00","09:40:00")
    to = TimeHelper.parseTimeOfDayExpression("09:40:10","09:40:10")
  }
  and "a pre-defined response from the facade for sendMinuteDataRequest", {

    response = new File("${fixtures()}/bp_HTT_response.txt").text.replaceAll("__DATE__",TimeHelper.toDayString(from))

    result = null
    facade = new MockIQFeedFacade()
    client = createClient(facade,config)
    facade.client = client
    client.debugTag="debug"
    //AtomicInteger
    facade.tickDataResponse = response
  }
  when "I call getMinuteBars on the client for BIDU", {
    ensureDoesNotThrow(Exception) {
      printExceptions {
        result = client.getSecondBars("BP", from, to, 100, TimeUnit.MILLISECONDS, 0)
      }
    }
  }
  then "the facade should have been called with the correct from argument", {
    facade.fromParam.shouldMatch(/(\d){8} 094000/)
  }
  then "the facade should have been called with the correct to argument", {
    facade.toParam.shouldMatch(/(\d){8} 094009/)
  }
  then "I should get ten bars", {
    ensure(result){isNotNull()}
    result.size().shouldBe(10)
  }
  and "the first bar should match the first second", {
    bar = result.getBar(0)
    "H $bar.highCents L $bar.lowCents O $bar.openCents C $bar.closeCents V $bar.volume".shouldBe("H 3212 L 3209 O 3212 C 3210 V 13300")
  }
  and "the last bar should match the last second", {
    bar = result.getBar(9)
    "H $bar.highCents L $bar.lowCents O $bar.openCents C $bar.closeCents V $bar.volume".shouldBe("H 3213 L 3212 O 3212 C 3213 V 13600")
  }
  and "the symbol should match", {
    result.symbol.shouldBe("BP")
  }
}

scenario "updating the splits DB", {
  given "a splits DB", {
    splits = new SplitsDb()
  }
  and "a fundamentals response for AAPL", {
    splitText = "0.50 07/09/2010,0.50 12/22/1997"
    facadeResponse = "F,AAPL,D,30.4,471000,83.9900,44.3700,83.9900,57.3750,0.30,0.05,0.24,08/23/2010,07/21/2010,1,7,,8075274,,2.56,3.00,35.2,12,,CORE LABORATORIES,CLB CLB1,54.,0.83,,390.5,106.4,06/30/2010,209.1,44676,213112,$splitText,,0,14,4,1389,25.41,1,7,07/22/2010,08/17/2009,07/22/2010,02/05/2010,59.06,,,,,213112,,"
  }
  and "the facade sends back the fundamentals field names", {
    fieldNames = "S,FUNDAMENTAL FIELDNAMES,Symbol,Exchange ID,PE,Average Volume,52 Week High,52 Week Low,Calendar Year High,Calendar Year Low,Dividend Yield,Dividend Amount,Dividend Rate,Pay Date,Ex-dividend Date,(Reserved),(Reserved),(Reserved),Short Interest,(Reserved),Current Year EPS,Next Year EPS,Five-year Growth Percentage,Fiscal Year End,(Reserved),Company Name,Root Option Symbol,Percent Held By Institutions,Beta,Leaps,Current Assets,Current Liabilities,Balance Sheet Date,Long-term Debt,Common Shares Outstanding,(Reserved),Split Factor 1,Split Factor 2,(Reserved),Market Center,Format Code,Precision,SIC,Historical Volatility,Security Type,Listed Market,52 Week High Date,52 Week Low Date,Calendar Year High Date,Calendar Year Low Date,Year End Close,Maturity Date,Coupon Rate,Expiration Date,Strike Proce,NAICS"
    facade = new MockIQFeedFacade()
    facade.onFundamentalsRequestResponse = {
      client.onAdminData(fieldNames)
      client.onLevelOneData(facadeResponse)
    }
    client = createClient(facade,config)
    client.debugTag="split" // FIXME DELETE THIS LINE!!!
  }
  when "updateSplitsFor(\"AAPL\",splits) is called", {
    def requestThread = {
      client.updateSplitsFor("AAPL",splits,100,TimeUnit.MILLISECONDS)
      //coord.finishTest()
    }
    ensureDoesNotThrow(Exception) {
      printExceptions {
        requestThread.call() // no background thread at all...
      }
    }
  }
  and "we wait a bit", {
    sleep(100)
  }
  then "the splits DB has records corresponding to the response", {
    result = splits.get("AAPL")
    result.size().shouldBe 2
    ensure("${result[0]}") { startsWith("12/22/1997,AAPL,0.5,,true,") }
    ensure("${result[1]}") { startsWith("07/09/2010,AAPL,0.5,,true,") }
  }
}
