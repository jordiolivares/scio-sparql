package es.jolivar.scio.sparql

import com.spotify.scio.ScioContext
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import es.jolivar.scio.sparql.TriplesIO.{ReadParams, WriteParams}
import org.apache.beam.sdk.io.{Compression, FileIO}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio.Rio
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler

import java.nio.channels.Channels
import scala.jdk.OptionConverters._
import scala.util.Using

object TriplesIO {
  case class ReadParams(
      compression: Compression = Compression.AUTO
  )
  case class WriteParams(
      compression: Compression = Compression.GZIP
  )
}

final case class TriplesIO(path: String)
    extends ScioIO[Statement]
    with Serializable {
  override type ReadP = ReadParams
  override type WriteP = WriteParams
  override val tapT: TapT[Statement] = EmptyTapOf[Statement]

  override protected def read(
      sc: ScioContext,
      params: ReadP
  ): SCollection[Statement] = {
    val getFiles = FileIO.`match`().filepattern(path)
    val readFiles = FileIO.readMatches().withCompression(params.compression)
    sc.customInput("Match Triple Files", getFiles)
      .applyTransform(readFiles)
      .applyTransform(ParDo.of(new FileToStatementsDoFn))
  }

  override protected def write(
      data: SCollection[Statement],
      params: WriteP
  ): Tap[tapT.T] = ???

  override def tap(read: ReadP): Tap[tapT.T] =
    UnsupportedTap("Tap not implemented yet")

  private class FileToStatementsDoFn
      extends DoFn[FileIO.ReadableFile, Statement]
      with Serializable {
    @ProcessElement
    def processElement(ctx: ProcessContext): Unit = {
      val element = ctx.element()
      Using.Manager { use =>
        val channel = use(element.open())
        val inputStream = use(Channels.newInputStream(channel))
        val parser = Rio
          .getParserFormatForFileName(
            element.getMetadata.resourceId().getFilename
          )
          .toScala
        parser.foreach { format =>
          val reader = Rio.createParser(format)
          reader.setRDFHandler(new AbstractRDFHandler {
            override def handleStatement(st: Statement): Unit =
              ctx.output(st)
          })
          reader.parse(inputStream)
        }
      }
    }
  }
}

object TriplesReader {
  implicit class ScioContextExts(val sc: ScioContext) extends AnyVal {
    def readTriples(path: String): SCollection[Statement] = {
      sc.read(new TriplesIO(path))(TriplesIO.ReadParams(Compression.AUTO))
    }
  }
}
