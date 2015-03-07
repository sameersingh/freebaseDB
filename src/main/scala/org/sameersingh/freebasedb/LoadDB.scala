package org.sameersingh.freebasedb

import java.io.{FileInputStream, PrintWriter}
import java.util.zip.GZIPInputStream

import scala.collection.immutable.HashSet

/**
 * @author sameer
 * @since 2/23/15.
 */
class LoadDB(val db: DB with Update, val baseDir: String) {
}

object LoadMongoDB extends LoadDB(new MongoIO, "/home/sameer/work/data/freebase/") {

  val rels = HashSet("people.person.spouse_s")

  def loadRelations(db: DB with Update, fname: String): Unit = {
    val source = io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(fname)))
    db.loadRelations(source.getLines().flatMap(l => {
      if (rels.exists(r => l.contains(r))) {
      val split = l.split("\\t")
      val s = ProcessFreebaseDump.stripRDF(split(0))
      val r = ProcessFreebaseDump.stripRDF(split(1))
      val o = ProcessFreebaseDump.stripRDF(split(2))
      if (rels contains r) {
        Some((s, r, o))
      } else None } else None
    }))
  }

  def main(args: Array[String]) {
    loadRelations(db, baseDir + "freebase-rdf-2014-07-06-00-00.gz")
//    print("Writing ids... ")
//    db.loadEntityIds(baseDir + "type.object.id.gz")
//    println("done.")
//
//    print("Writing names... ")
//    db.loadEntityNames(baseDir + "type.object.name.gz")
//    println("done.")
//
//    print("Writing images... ")
//    db.loadEntityImages(baseDir + "common.topic.image.gz")
//    println("done.")
//
//    print("Writing description... ")
//    db.loadEntityDescription(baseDir + "common.topic.description.gz")
//    println("done.")
//
//    print("Writing notable types... ")
//    db.loadEntityNotableTypes(baseDir + "common.topic.notable_types.gz")
//    println("done.")
//
//    print("Writing types... ")
//    db.loadEntityTypes(baseDir + "type.object.type.gz")
//    println("done.")
//
//    print("Writing aliases... ")
//    db.loadEntityAliases(baseDir + "common.topic.alias.gz")
//    println("done.")
//
//    print("Writing locations... ")
//    db.loadGeoLocation(baseDir + "location.location.geolocation.gz")
//    println("done.")
//
//    print("Writing longitudes... ")
//    db.loadLongitude(baseDir + "location.geocode.longitude.gz")
//    println("done.")
//
//    print("Writing latitudes... ")
//    db.loadLatitude(baseDir + "location.geocode.latitude.gz")
//    println("done.")
  }
}

object NamesToIds extends MongoIO() {
  def filter(mid: String): Boolean = {
    descriptions(Seq(mid)).values.size > 0
  }

  def prominence(mid: String): Double = {
    types(Seq(mid)).values.flatten.size.toDouble
  }

  def main(args: Array[String]): Unit = {
    val file = args(0)
    val output = args(1)
    val writer = new PrintWriter(output)
    val source = io.Source.fromFile(file)
    for (line <- source.getLines()) {
      val name = line.trim
      val allMs = mid(Seq(name)).values.flatten.toSeq.filter(filter _)
      if(!allMs.isEmpty) {
        val m = allMs.maxBy(m => prominence(m))
        val typs = types(Seq(m)).values.flatten.filter(t => !t.startsWith("user") && !t.startsWith("base") && !t.startsWith("common")).toSeq
        writer.println("%s\t%s\t%s".format(name, m, typs.mkString("\t")))
      }
    }
    writer.flush()
    writer.close()
  }
}

object FreebaseTypesFromIds extends MongoIO() {
  def main(args: Array[String]): Unit = {
    val file = args(0)
    val output = args(1)
    val writer = new PrintWriter(output)
    val source = io.Source.fromFile(file)
    for (line <- source.getLines()) {
      val mid = line.trim
      val typs = types(Seq(mid)).values.flatten.filter(t => !t.startsWith("user") && !t.startsWith("base") && !t.startsWith("common")).toSeq
      writer.println("%s\t%s".format(mid, typs.mkString("\t")))
    }
    writer.flush()
    writer.close()
  }
}