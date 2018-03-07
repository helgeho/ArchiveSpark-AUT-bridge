package de.l3s.archivespark.aut

import de.l3s.archivespark.ArchiveSpark
import de.l3s.archivespark.dataspecs.DataSpec
import de.l3s.archivespark.specific.warc.WarcLikeRecord
import de.l3s.archivespark.specific.warc.specs.{WarcCdxHdfsSpec, WarcGzHdfsSpec, WarcHdfsSpec}
import io.archivesunleashed.spark.archive.io.ArchiveRecord
import io.archivesunleashed.spark.rdd.RecordRDD._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object ArchiveSparkAUT {
  def loadArchives(path: String, sc: SparkContext, keepValidPages: Boolean): RDD[ArchiveRecord] = {
    val spec = if (path.toLowerCase.endsWith("arc.gz")) WarcGzHdfsSpec(path) else WarcHdfsSpec(path)
    loadArchives(sc, spec, keepValidPages)
  }

  def loadArchives(path: String, keepValidPages: Boolean): RDD[ArchiveRecord] = {
    val spec = if (path.toLowerCase.endsWith("arc.gz")) WarcGzHdfsSpec(path) else WarcHdfsSpec(path)
    loadArchives(spec, keepValidPages)
  }

  def loadArchives(cdxPath: String, warcPath: String, sc: SparkContext, keepValidPages: Boolean): RDD[ArchiveRecord] = {
    val spec = WarcCdxHdfsSpec(cdxPath, warcPath)
    loadArchives(sc, spec, keepValidPages)
  }

  def loadArchives(cdxPath: String, warcPath: String, keepValidPages: Boolean): RDD[ArchiveRecord] = {
    val spec = WarcCdxHdfsSpec(cdxPath, warcPath)
    loadArchives(spec, keepValidPages)
  }

  def loadArchives[Raw, Parsed <: WarcLikeRecord : ClassTag](sc: SparkContext, spec: DataSpec[Raw, Parsed], keepValidPages: Boolean): RDD[ArchiveRecord] = {
    val rdd: RDD[ArchiveRecord] = ArchiveSpark.load(sc, spec).map(r => new ArchiveSparkAUTRecord(r))
    if (keepValidPages) rdd.keepValidPages() else rdd
  }

  def loadArchives[Raw, Parsed <: WarcLikeRecord : ClassTag](spec: DataSpec[Raw, Parsed], keepValidPages: Boolean): RDD[ArchiveRecord] = {
    val rdd: RDD[ArchiveRecord] = ArchiveSpark.load(spec).map(r => new ArchiveSparkAUTRecord(r))
    if (keepValidPages) rdd.keepValidPages() else rdd
  }

  def loadArchives(path: String, sc: SparkContext): RDD[ArchiveRecord] = loadArchives(path, sc, keepValidPages = true)

  def loadArchives(path: String): RDD[ArchiveRecord] = loadArchives(path, keepValidPages = true)

  def loadArchives(cdxPath: String, warcPath: String, sc: SparkContext): RDD[ArchiveRecord] = loadArchives(cdxPath, warcPath, sc, keepValidPages = true)

  def loadArchives(cdxPath: String, warcPath: String): RDD[ArchiveRecord] = loadArchives(cdxPath, warcPath, keepValidPages = true)

  def loadArchives[Raw, Parsed <: WarcLikeRecord : ClassTag](sc: SparkContext, spec: DataSpec[Raw, Parsed]): RDD[ArchiveRecord] = loadArchives[Raw, Parsed](sc, spec, keepValidPages = true)

  def loadArchives[Raw, Parsed <: WarcLikeRecord : ClassTag](spec: DataSpec[Raw, Parsed]): RDD[ArchiveRecord] = loadArchives[Raw, Parsed](spec, keepValidPages = true)

  def load(path: String, sc: SparkContext, keepValidPages: Boolean): RDD[ArchiveRecord] = loadArchives(path, sc, keepValidPages)

  def load(path: String, keepValidPages: Boolean): RDD[ArchiveRecord] = loadArchives(path, keepValidPages)

  def load(cdxPath: String, warcPath: String, sc: SparkContext, keepValidPages: Boolean): RDD[ArchiveRecord] = loadArchives(cdxPath, warcPath, sc, keepValidPages)

  def load(cdxPath: String, warcPath: String, keepValidPages: Boolean): RDD[ArchiveRecord] = loadArchives(cdxPath, warcPath, keepValidPages)

  def load[Raw, Parsed <: WarcLikeRecord : ClassTag](sc: SparkContext, spec: DataSpec[Raw, Parsed], keepValidPages: Boolean): RDD[ArchiveRecord] = loadArchives[Raw, Parsed](sc, spec, keepValidPages)

  def load[Raw, Parsed <: WarcLikeRecord : ClassTag](spec: DataSpec[Raw, Parsed], keepValidPages: Boolean): RDD[ArchiveRecord] = loadArchives[Raw, Parsed](spec, keepValidPages)

  def load(path: String, sc: SparkContext): RDD[ArchiveRecord] = loadArchives(path, sc)

  def load(path: String): RDD[ArchiveRecord] = loadArchives(path)

  def load(cdxPath: String, warcPath: String, sc: SparkContext): RDD[ArchiveRecord] = loadArchives(cdxPath, warcPath, sc)

  def load(cdxPath: String, warcPath: String): RDD[ArchiveRecord] = loadArchives(cdxPath, warcPath)

  def load[Raw, Parsed <: WarcLikeRecord : ClassTag](sc: SparkContext, spec: DataSpec[Raw, Parsed]): RDD[ArchiveRecord] = loadArchives[Raw, Parsed](sc, spec)

  def load[Raw, Parsed <: WarcLikeRecord : ClassTag](spec: DataSpec[Raw, Parsed]): RDD[ArchiveRecord] = loadArchives[Raw, Parsed](spec)
}
