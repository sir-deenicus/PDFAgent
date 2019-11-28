import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.google.gson.Gson
import org.allenai.pdffigures2.{FigureExtractor, FigureType}
import org.apache.pdfbox.pdmodel.PDDocument
import technology.tabula.writers.CSVWriter

import collection.JavaConverters._
import technology.tabula
import technology.tabula.extractors.BasicExtractionAlgorithm
import technology.tabula.detectors.NurminenDetectionAlgorithm
import technology.tabula.ObjectExtractor

case class TableInfo(Caption: String, Table: String)

object Command extends Enumeration {
    val Default = Value(13)
    val UseTabula = Value(14)
    val Quit = Value(12)
}

object core {
    val figureExtractor = new FigureExtractor(
        allowOcr = true,
        detectSectionTitlesFirst = true,
        rebuildParagraphs = true,
        ignoreWhiteGraphics = true,
        cleanRasterizedFigureRegions = false
    )

    val detectionAlgorithm = new NurminenDetectionAlgorithm

    val bea = new BasicExtractionAlgorithm

    val gson = new Gson()

    def main(args: Array[String]): Unit = {
        openCommunicationChannel()
    }

    def runTabula(pdfDocument: PDDocument): Array[TableInfo] = {
        val extractor = new ObjectExtractor(pdfDocument)

        val pages = extractor.extract
        val rects =
            pages.asScala
                .map(page => (page, detectionAlgorithm.detect(page).asScala))
                .filter { case (_, rs) => rs.nonEmpty }
                .flatMap { case (page, rectangles) => rectangles.map(r => bea.extract(page.getArea(r))) }
                .toArray

        val tables =
            rects.map(tlist => {
                val sb = new java.lang.StringBuilder
                tlist.forEach(t => (new CSVWriter).write(sb, t))
                TableInfo(Caption = "", sb.toString)
            })

        extractor.close()
        tables
    }

    def runPDFFigures(pdf: PDDocument): Array[TableInfo] = {
        val r = figureExtractor.getFiguresWithText(pdf)
        val oextractor = new tabula.ObjectExtractor(pdf)
        val table =
            r.figures.map(f => {
                f.figType match {
                    case FigureType.Table =>
                        val p =
                            oextractor
                                .extract(f.page + 1)
                                .getArea(f.regionBoundary.y1.toFloat,
                                    f.regionBoundary.x1.toFloat,
                                    f.regionBoundary.y2.toFloat,
                                    f.regionBoundary.x2.toFloat)

                        val t = bea.extract(p)
                        val sb = new java.lang.StringBuilder
                        (new CSVWriter).write(sb, t)
                        TableInfo(Caption = f.caption, sb.toString)
                    case _ => TableInfo("", "")
                }
            }).filter(_.Caption != "").toArray
        oextractor.close()
        table
    }

    def openCommunicationChannel(): Unit = {
        var pipebox: Option[RandomAccessFile] = None

        var die = false
        while (!die) {
            pipebox match {
                case None =>
                    try {
                        val pipe =
                            new RandomAccessFile("\\\\.\\pipe\\pdfDaemon-commpipe", "rw")
                        println("Comm channel open.")
                        pipebox = Some(pipe)
                    } catch {
                        case _ => ()
                    }
                case Some(pipe) =>
                    val inp = pipe.readInt()
                    val comm = Command(inp)
                    comm match {
                        case Command.Quit => die = true
                        case _ =>
                            try {
                                val fsize = pipe.readInt()
                                val fileBytes = new Array[Byte](fsize)
                                val _ = pipe.read(fileBytes)

                                val doc = PDDocument.load(fileBytes)
                                val tables = comm match {
                                    case Command.UseTabula => runTabula(doc)
                                    case _ => runPDFFigures(doc)
                                }

                                doc.close()

                                val tbytes = gson.toJson(tables).getBytes(StandardCharsets.UTF_8)

                                val plen = ByteBuffer.allocate(4).putInt(tbytes.length).array()
                                pipe.write(plen)
                                pipe.write(tbytes)
                            } catch {
                                case e: Throwable =>
                                    val errmsg = "[ERROR]: " + e.getMessage
                                    val txtbytes = errmsg.getBytes(StandardCharsets.UTF_8)
                                    val plen =
                                        ByteBuffer.allocate(4).putInt(txtbytes.length).array()
                                    pipe.write(plen)
                                    pipe.write(txtbytes)
                                    println(errmsg)
                            }
                    }
            }
        }
        pipebox.foreach(_.close)
    }
}
