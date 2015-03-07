package org.sameersingh.freebasedb

/**
 * Query only interface for freebase
 * @author sameer
 * @since 2/23/15.
 */
trait DB {

  def mid(names: Seq[String]): scala.collection.Map[String, Seq[String]]

  def ids(mids: Seq[String]): scala.collection.Map[String, String]

  def alias(mids: Seq[String]): scala.collection.Map[String, Seq[String]]

  def names(mids: Seq[String]): scala.collection.Map[String, String]

  def images(mids: Seq[String]): scala.collection.Map[String, String]

  def descriptions(mids: Seq[String]): scala.collection.Map[String, String]

  def types(mids: Seq[String]): scala.collection.Map[String, Seq[String]]

  def notableTypes(mids: Seq[String]): scala.collection.Map[String, String]

  def relation(subj: Option[String], rel: Option[String], obj: Option[String]): Seq[(String, String, String)] = throw new Error("not implemented")

  def logitude(mids: Seq[String]): scala.collection.Map[String, Double] = throw new Error("not implemented")

  def latitude(mids: Seq[String]): scala.collection.Map[String, Double] = throw new Error("not implemented")
}

/**
 * Support for loading freebase stuff into the DB
 */
trait Update extends DB {

  def loadEntityIds(file: String): Unit
  def loadEntityNames(file: String): Unit
  def loadEntityAliases(file: String): Unit
  def loadEntityImages(file: String): Unit
  def loadEntityDescription(file: String): Unit
  def loadEntityNotableTypes(file: String): Unit
  def loadEntityTypes(file: String): Unit
  def loadGeoLocation(file: String): Unit
  def loadLongitude(file: String): Unit
  def loadLatitude(file: String): Unit
  def loadRelations(rels: Iterator[(String, String, String)]): Unit
}