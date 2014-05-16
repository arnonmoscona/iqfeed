import org.mockito.Mockito   //FIXME should be replaced by jMockit, probably
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

import jconch.testing.TestCoordinator

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
