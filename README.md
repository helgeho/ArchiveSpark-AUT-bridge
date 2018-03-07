# [ArchiveSpark](https://github.com/helgeho/ArchiveSpark)-[AUT](https://github.com/archivesunleashed/aut)-bridge

This bridge provides a compatibility layer between [ArchiveSpark](https://github.com/helgeho/ArchiveSpark) and [The Archives Unleashed Toolkit (AUT)]((https://github.com/helgeho/ArchiveSpark)). You can now use AUT's intuitive fluent interface in combination with ArchiveSpark efficient loading and filtering approach.

In order to use it, simply replace `io.archivesunleashed.spark.matchbox.RecordLoader` with `de.l3s.archivespark.aut.ArchiveSparkAUT` in your AUT code wherever you load your data:

The following example from the [AUT documentation](http://archivesunleashed.org/aut/):    

```scala
import io.archivesunleashed.spark.matchbox._
import io.archivesunleashed.spark.rdd.RecordRDD._

RecordLoader.loadArchives("example.arc.gz", sc)
  .keepValidPages()
  .map(r => ExtractDomain(r.getUrl))
  .countItems()
  .take(10)
```

now becomes:

```scala
import io.archivesunleashed.spark.rdd.RecordRDD._
import io.archivesunleashed.spark.matchbox._
import de.l3s.archivespark.aut._

ArchiveSparkAUT.loadArchives("example.arc.gz", sc)
  .keepValidPages()
  .map(r => ExtractDomain(r.getUrl))
  .countItems()
  .take(10)
```

In order to benefit from ArchiveSpark's efficient [two-step loading approach](https://github.com/helgeho/ArchiveSpark#approach), please provide CDX records together with your (W)ARC files:

```scala
import io.archivesunleashed.spark.rdd.RecordRDD._
import io.archivesunleashed.spark.matchbox._
import de.l3s.archivespark.aut._

ArchiveSparkAUT.loadArchives("path/to/metadata.cdx.gz", "path/to/warc_dir")
  .keepValidPages()
  .map(r => ExtractDomain(r.getUrl))
  .countItems()
  .take(10)
```

*If you don't have CDX records for your dataset yet, you can read [here](https://github.com/helgeho/ArchiveSpark/blob/master/notebooks/Generate_CDX.ipynb) how to generate them with ArchiveSpark*

It is also possible to use any [ArchiveSpark Data Specification (DataSpec)](https://github.com/helgeho/ArchiveSpark/blob/master/docs/DataSpecs.md) for Web archives with the ArchiveSpark-AUT-bridge:

```scala
import io.archivesunleashed.spark.rdd.RecordRDD._
import io.archivesunleashed.spark.matchbox._
import de.l3s.archivespark.aut._
import de.l3s.archivespark.specific.warc.specs._

ArchiveSparkAUT.load(WaybackSpec("nytimes.com", from = 2012))
  .keepValidPages()
  .map(r => ExtractDomain(r.getUrl))
  .countItems()
  .take(10)
```

Finally, the bridge RDD can be converted back into an ArchiveSpark RDD at any time by calling `.toArchiveSpark` (only before the records are mapped to some plain values). This way, ArchiveSpark's [Enrich Functions](https://github.com/helgeho/ArchiveSpark/blob/master/docs/EnrichFuncs.md) and [operations](https://github.com/helgeho/ArchiveSpark/blob/master/docs/Operations.md) can be used:

```scala
import io.archivesunleashed.spark.rdd.RecordRDD._
import de.l3s.archivespark.aut._
import de.l3s.archivespark.enrich.functions._
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.specific.warc.specs._

ArchiveSparkAUT.load(WarcCdxHdfsSpec("path/to/metadata.cdx.gz", "path/to/warc_dir"))
  .keepValidPages()
  .toArchiveSpark
  .enrich(Html.first("title"))
  .saveAsJson("recordsWithTitle.json.gz")
```

The conversion also works the other way around:

```scala
import de.l3s.archivespark._
import io.archivesunleashed.spark.rdd.RecordRDD._
import io.archivesunleashed.spark.matchbox._
import de.l3s.archivespark.aut._
import de.l3s.archivespark.specific.warc.specs._

ArchiveSpark.load(WarcCdxHdfsSpec("path/to/metadata.cdx.gz", "path/to/warc_dir"))
  .filter(_.timestamp.startsWith("2012"))
  .toAUT
  .keepValidPages()
  .map(r => ExtractDomain(r.getUrl))
  .countItems()
  .take(10)
```

For more information on how to use and install ArchiveSpark, please [read the docs](https://github.com/helgeho/ArchiveSpark/blob/master/docs/README.md).

To learn about what you can do with ArchiveSpark, have a look at our [Jupyter recipes](https://github.com/helgeho/ArchiveSpark/blob/master/docs/Recipes.md ).

### License

The MIT License (MIT)

Copyright (c) 2018 Helge Holzmann ([L3S](http://www.L3S.de))

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.