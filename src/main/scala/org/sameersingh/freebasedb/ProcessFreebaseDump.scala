package org.sameersingh.freebasedb

import java.io.{FileInputStream, FileOutputStream, PrintWriter}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.immutable.HashMap

/**
 * Created by sameer on 3.5.15.
 */
object ProcessFreebaseDump {
  val attrs = Set(
    "type.object.type", "type.object.id", "type.object.name",
    "common.topic.alias", "common.topic.description", "common.topic.image", "wikipedia.en", "wikipedia.en_id",
    "common.topic.notable_types",
    "location.geocode.latitude", "location.geocode.longitude")
  val rels = Set("people.person.spouse_s", "people.person.nationality", "people.person.profession",
    "people.person.children", "people.person.parents", "people.person.education", "education.education.institution",
    "organization.membership_organization.members", "organization.organization_membership.member",
    "organization.organization.parent", "organization.organization_relationship.parent",
    "people.person.employment_history", "business.employment_tenure.company",
    "people.person.place_of_birth", "organization.organization.headquarters", "location.mailing_address.state_province_region",
    "organization.organization.headquarters", "location.mailing_address.citytown",
    "location.mailing_address.postal_code", "location.postal_code.country",
    "people.person.sibling_s", "people.sibling_relationship.sibling",
    "people.person.spouse_s", "people.marriage.spouse",
    "people.deceased_person.place_of_death",
    "people.person.places_lived", "people.place_lived.location",
    "organization.organization.place_founded",
    "organization.organization.companies_acquired", "business.acquisition.company_acquired",
    "organization.organization.locations",
    "organization.organization.board_members",
    "organization.organization_board_membership.member",
    "government.governmental_jurisdiction.governing_officials", "government.government_position_held.office_holder",
    "government.governmental_jurisdiction.government_bodies", "location.administrative_division.country",
    "location.location.adjoin_s", "location.adjoining_relationship.adjoins",
    "travel.travel_destination.tourist_attractions", "travel.travel_destination.local_transportation",
    "location.location.nearby_airports", "periodicals.newspaper_circulation_area.newspapers",
    "sports.sports_team_location.teams", "celebrities.celebrity.celebrity_friends",
    "celebrities.friendship.friend", "influence.influence_node.influenced_by", "organization.organization.partnerships",
    "organization.organization_partnership.members", "venture_capital.venture_investor.investments",
    "venture_capital.venture_investment.company", "location.location.containedby", "location.location.contains", "location.location.geolocation",
    "organization.organization_founder.organizations_founded", "organization.organization.founders",
    "organization.organization_membership.organization", "organization.organization_member.member_of"
  )

  class Rels(baseDir: String) {
    val map = HashMap(rels.map(r => r-> new Rel(r, baseDir + r +".gz")).toSeq:_*)
    def +=(l: String) = {
      val split = l.split("\\t")
      val s = stripRDF(split(0))
      val r = stripRDF(split(1))
      val o = stripRDF(split(2))
      if (rels contains r) {
        assert(split.take(3).toSeq.forall(_.startsWith("<http://rdf.freebase.com/ns/")), l)
        map(r) += (s,o)
      }
  }
    def stats = map.values.toSeq.sortBy(_.r).map(rel => rel.stats).mkString("\n")
    def close = map.values.foreach(_.close)
    def flush = map.values.foreach(_.flush)
  }

  class Rel(val r: String, f: String) {
    val w = new PrintWriter(new GZIPOutputStream(new FileOutputStream(f)))
    var count = 0
    def +=(subj: String, obj: String): Unit = {
      count += 1
      w.println("%s\t%s".format(subj, obj))
    }
    def stats = "%s:\t%d".format(r, count)
    def flush = w.flush()
    def close = { w.flush(); w.close() }
  }

  def main(args: Array[String]): Unit = {
    val baseDir = "/home/sameer/work/data/freebase/"
    val rs = new Rels(baseDir)
    val source = io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(baseDir + "freebase-rdf-2014-07-06-00-00.gz")))
    var idx = 0l
    for(l <- source.getLines()) {
      if (rels.exists(r => l.contains(r))) rs += l
      if(idx % 10000000l == 0) {
        println()
        println(rs.stats)
        print("%10d :\t".format(idx))
        rs.flush
      }
      if(idx % 100000l == 0) print(".")
      idx += 1l
    }
    source.close
    rs.close
  }

  def stripRDF(url: String): String = {
    if (url.startsWith("<http://rdf.freebase.com") && url.endsWith(">"))
    url.replaceAll("<http://rdf.freebase.com/ns/", "").replaceAll("<http://rdf.freebase.com/key/", "").dropRight(1)
    else url
  }
}
