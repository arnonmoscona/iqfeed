package com.moscona.test.easyb

import com.moscona.exceptions.InvalidArgumentException
import com.moscona.test.util.TestResourceHelper
import com.moscona.trading.adapters.iqfeed.DtnIQFeedFacade
import com.moscona.trading.adapters.iqfeed.UserOverrides
import com.moscona.trading.formats.deprecated.MarketTree
import com.moscona.trading.formats.deprecated.MasterTree
import com.moscona.trading.persistence.SplitsDb
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils

/**
 * Created by Arnon on 5/6/2014.
 *
 * WARNING WARNING WARNING: for now there are multiple copies of this file in multiple projects.
 * This file is only for use with legacy EasyB test code.
 * I don't have an easy way for now to share it across projects. This fix is deferred.
 *
 * All EasyB test should be rewritten using some other framework.
 */

class TestHelper {
  public static final URL ANCHOR_URL = DtnIQFeedFacade.class.getResource("."); // FIXME Not only I need a copy, I also have to modify it UGH!
  private static TestResourceHelper testResourceHelper = null;

  static getTestResourceHelper() {

    if (testResourceHelper == null) {
      testResourceHelper = new TestResourceHelper(TestResourceHelper.DEFAULT_ROOT_MARKER, ANCHOR_URL as URL);
    }
    testResourceHelper
  }

  static {
    // number.shouldBeCloseTo(expected,tolerance)
    if (! Number.metaClass.methods*.name.contains("shouldBeCloseTo")) {
      Number.metaClass.shouldBeCloseTo = { expected, tolerance ->
        if (Math.abs(expected-delegate) > tolerance) {
          throw new org.easyb.exception.VerificationException("Expected $delegate to be close to $expected (within $tolerance) but the difference is ${expected-delegate}")
        }
      }
    }

//    // number.shouldBe(expected)
//    if (! Number.metaClass.methods*.name.contains("shouldBe")) {
//      Number.metaClass.shouldBe = { expected->
//        if (delegate==null || !delegate.equals(expected)) {
//          throw new org.easyb.exception.VerificationException("Expected $delegate to be $expected")
//        }
//      }
//    }

    // string.shouldMatch(/regexp/)
    if (! String.metaClass.methods*.name.contains("shouldMatch")) {
      String.metaClass.shouldMatch = { regexp ->
        if (!(delegate =~ regexp).matches()) {
          throw new org.easyb.exception.VerificationException("Expected '$delegate' to match /$regexp/")
        }
      }
    }

    // string.shouldBe(String)
    if (! String.metaClass.methods*.name.contains("shouldBe")) {
      String.metaClass.shouldBe = { value ->
        if (!(delegate.equals(value.toString()) )) {
          def diffAt = StringUtils.indexOfDifference(delegate as String,value.toString());
          def diffStr = "expected: $value\n          ${"-"*diffAt+"|"}\n     got: $delegate\n"
          throw new org.easyb.exception.VerificationException("Expected '$delegate' to equal '$value'\n\n$diffStr")
        }
      }
    }

    // string.shouldBe(String)
    if (! GString.metaClass.methods*.name.contains("shouldBe")) {
      GString.metaClass.shouldBe = { value ->
        if (!(delegate.toString().equals(value.toString()) )) {
          def diffAt = StringUtils.indexOfDifference(delegate as String,value.toString());
          def diffStr = "expected: $value\n          ${"-"*diffAt+"|"}\n     got: $delegate\n"
          throw new org.easyb.exception.VerificationException("Expected '$delegate' to equal '$value'\n\n$diffStr")
        }
      }
    }

    // file.listRecursive()
    if (! File.metaClass.methods*.name.contains("listRecursive")) {
      File.metaClass.listRecursive = {
        def retval = []
        delegate.eachFile{f->
          retval << f.canonicalPath
        }
        delegate.eachDir{f->
          retval << f.canonicalPath
          retval += f.listRecursive()
        }
        retval
      }
    }
  }


  static measureTiming(Closure yield) {
    def begin = System.currentTimeMillis()
    yield.call()
    (System.currentTimeMillis() - begin)/1000.0
  }

  // IMPORTANT: this method was removed as now I am dealing with multiple projects that cannot be identified by a single class
//  static projectRoot() {
//    projectRoot(MarketTree)
//  }

  static userHome() {
    System.getProperty("user.home")
  }

  static tmpDir() {
    def retval = projectRoot()+"/target/tmp"
    def f = new File(retval)
    if (! f.exists()) {
      f.mkdir()
    }
    retval
  }

  static tempFile(prefix="temp_",suffix=".tmp") {
    File.createTempFile(prefix, suffix, new File(tmpDir()))
  }

  static emptyTempDir(prefix="temp_",suffix=".tmpDir") {
    def file = tempFile(prefix, suffix)
    file.delete()
    file.mkdirs()
    file
  }

  static makeTempDir(prefix="temp_",suffix="Dir") {
    File f = File.createTempFile(prefix, suffix, new File(tmpDir()))
    f.delete()
    f.mkdir()
    return f
  }

  static clearTmpDir() {
    deleteAllRecursively(tmpDir())
  }

  static deleteAllRecursively(dir) {
    def list = []
    def dirFile = dir instanceof String ? new File(dir) : dir
    dirFile.eachFileRecurse {
      if (it != ".gitignore") {
        list << it
      }
    }
    list.sort().reverse().each {
      it.delete()
    }
    //FileUtils.deleteQuietly(dirFile as File)
  }

  static projectRoot() {
    getTestResourceHelper().projectRootPath
  }

  static fixtures() {
    projectRoot() + "/src/test/fixtures"
  }

  static printExceptions(Closure code) {
    try {
      code.run()
    } catch (Throwable e) {
      println "!"*80
      System.err.println("Exception: $e")
      //e.printStackTrace(System.err)
      printFilteredStackTrace(e)
      if (e.cause != null) {
        println "."*80
        println "Caused by $e.cause"
        println "."*80
        printFilteredStackTrace("$e.cause")
      }
      println "!"*80
      throw e
    }
  }

  static printFilteredStackTrace(Throwable t) {
    if (t==null) {
      return;
    }

    def err = System.err
    err.println t.toString()
    t.getStackTrace().each {
      if(it.className =~ /moscona/) {
        err.println "    at $it"
      }
      else if(it.className =~ /dataSpace/) {
        err.println "    at $it"
      }
      else if(it.fileName =~ /\.story/) {
        err.println "    at $it"
      }
      else if(it.fileName =~ /TestHelper/) {
        err.println "    at $it"
      }
      else if(it.fileName =~ /Mock/) {
        err.println "    at $it"
      }
//      else {
//        err.println "\t\t-- $it"
//      }
    }
  }



  public static def copyFromMaster(master, working) {
    def fixture_file = "${fixtures()}/$working"
    def from = "${fixtures()}/masters/$master"
    FileUtils.copyFile(new File(from), new File(fixture_file))
    return fixture_file
  }


  // ===========================================================================================================
  // Extending EasyB itself

  static ensureDoesNotThrow(Class clazz,Closure closure) throws Exception {
    Throwable ex = null;
    try {
      closure.call()
    }
    catch(Throwable t) {
      ex = t
    }

    if (ex!=null) {
      if (clazz.isAssignableFrom(ex.class)) {
        System.err.println("Unexpected exception: $ex")
        printFilteredStackTrace(ex)
        throw new org.easyb.exception.VerificationException("Expected not to throw $clazz.name but ${ex.getClass().name} was thrown anyway ($ex)")
      }
    }

  }

  static ensureFileExists(path) throws Exception {
    if (! new File(path as String).exists()) {
      throw new org.easyb.exception.VerificationException("Expected file \"$path\" to exist, but it does not")
    }
  }

  static ensureFloatClose(actual, expected, tolerance) throws org.easyb.exception.VerificationException {
    if (Math.abs(expected-actual) > tolerance) {
      throw new org.easyb.exception.VerificationException("Expected $actual to be close to $expected (within $tolerance) but the difference is ${expected-actual}")
    }
  }

  static void waitForCondition(String name,long timeout,Closure closure) {
    def start = System.currentTimeMillis();
    while (! closure.call()) {
      if (System.currentTimeMillis()-start > timeout) {
        def error = "Timed out while waiting for condition '$name'"
        System.err.println("Timeout ($timeout msec): $error");
        throw new org.easyb.exception.VerificationException(error)
      }
      sleep(50)
    }
  }

  static void deleteFileIfExists(path) {
    printExceptions {
      File file = new File(path as String)
      if (file.exists()) {
        file.delete()
      }
    }
  }

  static List loadSimpleCsv(path, boolean ignoreFirstLine) {
    File file = new File(path as String)
    def retval = []
    def isFirstLine = true
    file.eachLine {
      if (!isFirstLine || ! ignoreFirstLine) {
        retval << it.split(",")
      }
      isFirstLine = false
    }
    retval
  }

  static Object round(obj, precision) {
    if (obj==null) {
      return null
    }
    if (obj.respondsTo("round")) {
      return obj.round(precision)
    }
    obj
  }

  static boolean shouldRunUnsafeScenarios() {
    return (System.properties["easyb_safe_mode"] ?: "true") != "true"   // unless safe mode is turned off then it's on
  }


  static boolean isBlank(s) {
    s==null || s.toString().trim().equals("")
  }

  static List<List<Double>> loadMatrix(file, delimiter) {
    List<List<Double>> matrix = new ArrayList<List<Double>>()
    def size = null
    def lineNo=0
    new File(file as String).eachLine { line ->
      lineNo++
      if (!isBlank(line)) {
        def list = line.trim().split(" ").collect {it as double}
        if (size==null) {
          size = list.size()
        }
        if (list.size() != size) {
          throw new InvalidArgumentException("A matrix must have uniform row size. Expected $size but got ${list.size()} at line #$lineNo of $file")
        }
        matrix.add(list)
      }
    }
    matrix
  }

  static loadMarketTreeFixture(master = "market_tree.csv", working = "market_tree.csv") {
    String fixture_file = copyFromMaster(master, working)
    def valid_tree = null;
    printExceptions {
      valid_tree = new MarketTree().load(fixture_file)
    }
    valid_tree
  }

  static loadMasterTreeFixture(master = "master_tree.csv", working = "master_tree.csv") {
    String fixture_file = copyFromMaster(master, working)
    def valid_tree = null;
    printExceptions {
      valid_tree = new MasterTree().load(fixture_file)
    }
    valid_tree
  }

  static reasonableUserOverrides() {
    return new UserOverrides(
        marketTreePath: "${fixtures()}/market_tree.csv",
        tickStreamSimulatorDataPath: "${fixtures()}/1sec_data",
        loggingAlertServiceLogFile: "#SystemProperty{user.home}/intellitrade_alerts.log",
        streamDataStoreRoot: tmpDir()
    )
  }
}
