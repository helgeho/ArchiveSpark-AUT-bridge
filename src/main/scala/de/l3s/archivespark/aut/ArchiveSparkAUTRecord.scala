package de.l3s.archivespark.aut

import de.l3s.archivespark.enrich.dataloads.ByteContentLoad
import de.l3s.archivespark.enrich.functions._
import de.l3s.archivespark.enrich.{EnrichFunc, EnrichRoot}
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.specific.warc.WarcLikeRecord
import io.archivesunleashed.spark.archive.io._
import io.archivesunleashed.spark.matchbox.ExtractDate.DateComponent
import io.archivesunleashed.spark.matchbox.{ExtractDate, ExtractDomain}

class ArchiveSparkAUTRecord (private var self: WarcLikeRecord) extends ArchiveRecord {
  def toArchiveSpark: WarcLikeRecord = self

  private def enrich[Root <: EnrichRoot](func: EnrichFunc[Root, _]): WarcLikeRecord = {
    self = func.enrich(self.asInstanceOf[Root]).asInstanceOf[WarcLikeRecord]
    self
  }

  override def getCrawlDate: String = ExtractDate(self.get.timestamp, DateComponent.YYYYMMDD)

  override def getCrawlMonth: String = ExtractDate(self.get.timestamp, DateComponent.YYYYMM)

  override def getContentBytes: Array[Byte] = enrich(DataLoad(ByteContentLoad.Field)).get[Array[Byte]](ByteContentLoad.Field).get

  override def getContentString: String = enrich(StringContent).getValue(StringContent)

  override def getMimeType: String = self.mime

  override def getUrl: String = self.originalUrl

  override def getDomain: String = ExtractDomain(getUrl)

  override def getImageBytes: Array[Byte] = getContentBytes
}

object ArchiveSparkAUTRecord {
  implicit def toArchiveSparkRecord(record: ArchiveSparkAUTRecord): WarcLikeRecord = record.toArchiveSpark
  implicit def toAUTRecord[A <: WarcLikeRecord](record: A): ArchiveSparkAUTRecord = new ArchiveSparkAUTRecord(record)
}