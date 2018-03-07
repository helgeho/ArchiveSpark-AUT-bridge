package de.l3s.archivespark

import de.l3s.archivespark.specific.warc.WarcLikeRecord
import org.apache.spark.rdd.RDD
import io.archivesunleashed.spark.archive.io._

package object aut {
  implicit def autToArchiveSparkRdd(rdd: RDD[ArchiveRecord]): RDD[WarcLikeRecord] = rdd.map(_.asInstanceOf[ArchiveSparkAUTRecord].toArchiveSpark)

  implicit def archiveSparkToAutRecord[A <: WarcLikeRecord](record: A): ArchiveRecord = ArchiveSparkAUTRecord.toAUTRecord(record)

  implicit def archiveSparkToAutRdd(rdd: RDD[_ <: WarcLikeRecord]): RDD[ArchiveRecord] = rdd.map(ArchiveSparkAUTRecord.toAUTRecord)

  implicit class AutRDD(rdd: RDD[ArchiveRecord]) {
    def toArchiveSpark: RDD[WarcLikeRecord] = autToArchiveSparkRdd(rdd)
  }

  implicit class ArchiveSparkRDD(rdd: RDD[_ <: WarcLikeRecord]) {
    def toAUT: RDD[ArchiveRecord] = archiveSparkToAutRdd(rdd)
  }
}
