package org.sameersingh.freebasedb

/**
 * @author sameer
 * @since 2/23/15.
 */
class LoadDB(val db: DB with Update, val baseDir: String) {
}

object LoadMongoDB extends LoadDB(new MongoIO, "/home/sameer/work/data/freebase/") {

  def main(args: Array[String]) {
    print("Writing ids... ")
    db.loadEntityIds(baseDir + "type.object.id.gz")
    println("done.")

    print("Writing names... ")
    db.loadEntityNames(baseDir + "type.object.name.gz")
    println("done.")

    print("Writing images... ")
    db.loadEntityImages(baseDir + "common.topic.image.gz")
    println("done.")

    print("Writing description... ")
    db.loadEntityDescription(baseDir + "common.topic.description.gz")
    println("done.")

    print("Writing notable types... ")
    db.loadEntityNotableTypes(baseDir + "common.topic.notable_types.gz")
    println("done.")

    print("Writing types... ")
    db.loadEntityTypes(baseDir + "type.object.type.gz")
    println("done.")

    print("Writing types... ")
    db.loadEntityAliases(baseDir + "common.topic.alias.gz")
    println("done.")

    print("Writing locations... ")
    db.loadGeoLocation(baseDir + "location.location.geolocation.gz")
    println("done.")

    print("Writing longitudes... ")
    db.loadLongitude(baseDir + "location.geocode.longitude.gz")
    println("done.")

    print("Writing latitudes... ")
    db.loadLatitude(baseDir + "location.geocode.latitude.gz")
    println("done.")
  }
}
