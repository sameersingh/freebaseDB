package org.sameersingh.freebasedb

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

import com.mongodb.casbah.Imports._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/**
 * @author sameer
 * @since 2/23/15.
 */
class MongoIO(host: String = "localhost", port: Int = 27017) extends DB with Update {
  val dbName = "freebase"

  val client = MongoClient(host, port)
  val db = client.getDB(dbName)

  class MongoInsertBuffer(val coll: MongoCollection, val size: Int) {
    val buffer = new ArrayBuffer[MongoDBObject]

    def insert(d: MongoDBObject) {
      buffer += d
      if (buffer.size >= size) forceInsert()
    }

    def insertAll(ds: Iterable[MongoDBObject]) {
      buffer ++= ds
      if (buffer.size >= size) forceInsert()
    }

    def forceInsert() {
      //print("mongo: inserting %d objects... " format (buffer.size))
      coll.insert(buffer: _*)
      //println("done.")
      buffer.clear()
    }
  }

  def loadFile(fname: String, mongoCollName: String,
               arg2name: String,
               arg1name: String = "entity",
               arg1Strip: String => String = stripRDF,
               arg2Strip: String => String = stripRDF,
               arg1Index: Boolean = true,
               arg2Index: Boolean = false,
               filter: Array[String] => Boolean = x => true,
               clean: String => Any = x => x): Unit = {
    val buffer = new MongoInsertBuffer(db(mongoCollName), 100000)
    val source = io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(fname)))
    for (l <- source.getLines()) {
      val split = l.split("\\t")
      split(0) = arg1Strip(split(0))
      split(2) = arg2Strip(split(2))
      if (filter(split)) {
        val d = MongoDBObject(arg1name -> split(0), arg2name -> clean(split(2)))
        buffer.insert(d)
      }
    }
    source.close()
    buffer.forceInsert()
    if (arg1Index) buffer.coll.createIndex(MongoDBObject(arg1name -> 1))
    if (arg2Index) buffer.coll.createIndex(MongoDBObject(arg2name -> 1))
  }

  def mArgAnyPairFilter(split: Array[String]): Boolean = split(0).startsWith("m.")

  def mArgTypePairFilter(split: Array[String]): Boolean = {
    (split(0).startsWith("m.") && split(2).startsWith("\"") && split(2).endsWith("\"") && !split(2).startsWith("\"/user") && !split(2).startsWith("\"/soft"))
  }

  def mArgEnTextPairFilter(split: Array[String]): Boolean = {
    (split(0).startsWith("m.") && split(2).endsWith("@en"))
  }

  def bothMFilter(split: Array[String]): Boolean = {
    (split(0).startsWith("m.") && split(2).startsWith("m."))
  }

  def cleanDblValue(value: String): Double = value.drop(1).dropRight(1).toDouble

  def cleanEnText(value: String): String = value.drop(1).dropRight(4)

  def loadEntityImages(fname: String) = loadFile(fname, "entityImages", "img", filter = bothMFilter)

  def loadGeoLocation(fname: String) = loadFile(fname, "geoLocation", "geo", filter = bothMFilter)

  def loadLongitude(fname: String) = loadFile(fname, "geoLongitude", "longitude", arg1name = "geo", arg2Strip = x => x, clean = cleanDblValue, filter = mArgAnyPairFilter)

  def loadLatitude(fname: String) = loadFile(fname, "geoLatitude", "latitude", arg1name = "geo", arg2Strip = x => x, clean = cleanDblValue, filter = mArgAnyPairFilter)

  def loadEntityNames(fname: String) = loadFile(fname, "entityNames", "name", filter = mArgEnTextPairFilter, arg2Strip = x => x, clean = cleanEnText, arg2Index = true)

  def loadEntityAliases(fname: String) = loadFile(fname, "entityAliases", "alias", filter = mArgEnTextPairFilter, arg2Strip = x => x, clean = cleanEnText, arg2Index = true)

  def loadEntityIds(fname: String) = loadFile(fname, "entityIds", "id", filter = mArgTypePairFilter, clean = cleanEnText, arg2Strip = x => x)

  def loadEntityDescription(fname: String) = loadFile(fname, "entityDescription", "desc", filter = mArgEnTextPairFilter, clean = cleanEnText, arg2Strip = x => x)

  def loadEntityNotableTypes(fname: String) = loadFile(fname, "entityNotableTypes", "notabType", filter = bothMFilter)

  def loadEntityTypes(fname: String) = loadFile(fname, "entityTypes", "type", filter = mArgAnyPairFilter, arg2Index = true)

  def readOneFromDB(args: Seq[String],
                 collName: String,
                 value: DBObject => String,
                 argName: String = "entity"): scala.collection.Map[String, String] = {
    val coll = db(collName)
    val result = new mutable.HashMap[String, String]
    for (mid <- args) {
      coll.findOne(argName $eq mid).foreach(o => result(mid) = value(o))
    }
    result
  }

  def readAllFromDB(args: Seq[String],
                    collName: String,
                    value: DBObject => String,
                    argName: String = "entity"): scala.collection.Map[String, Seq[String]] = {
    val coll = db(collName)
    val result = new mutable.HashMap[String, Seq[String]]
    for (mid <- args) {
      result(mid) = coll.find(argName $eq mid).map(o => value(o)).toSeq
    }
    result
  }

  def alias(mids: Seq[String]): scala.collection.Map[String, Seq[String]] = readAllFromDB(mids, "entityAliases", o => o.get("alias").toString)

  def names(mids: Seq[String]): scala.collection.Map[String, String] = readOneFromDB(mids, "entityNames", o => o.get("name").toString)

  def images(mids: Seq[String]): scala.collection.Map[String, String] = readOneFromDB(mids, "entityImages", o => o.get("img").toString)

  def descriptions(mids: Seq[String]): scala.collection.Map[String, String] = readOneFromDB(mids, "entityDescription", o => o.get("desc").toString)

  def notableTypes(mids: Seq[String]): scala.collection.Map[String, String] = readOneFromDB(mids, "entityNotableTypes", o => o.get("notabType").toString)

  def types(mids: Seq[String]): scala.collection.Map[String, Seq[String]] = readAllFromDB(mids, "entityTypes", o => o.get("type").toString)
}