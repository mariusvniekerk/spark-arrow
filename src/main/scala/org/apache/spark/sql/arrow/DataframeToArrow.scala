package org.apache.spark.sql.arrow

import java.io.FileOutputStream
import java.nio.charset.{Charset, StandardCharsets}

import org.apache.arrow.memory.{BaseAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl
import org.apache.arrow.vector.complex.writer.BaseWriter.{ListWriter, MapWriter}
import org.apache.arrow.vector.complex.writer._
import org.apache.arrow.vector.file.ArrowWriter
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object DataframeToArrow {

  val UTF8 = Charset.forName("UTF-8")

  // TODO: Consider changing output tp BaseWriter, Option[Seq[BaseWriter]]
  def getChildWriter(parentWriter: MapWriter, col: StructField): BaseWriter = {
    col.dataType match {
      case NullType | BooleanType => parentWriter.bit(col.name)
      case ByteType => parentWriter.tinyInt(col.name)
      case ShortType => parentWriter.smallInt(col.name)
      case IntegerType => parentWriter.integer(col.name)
      case DateType => parentWriter.date(col.name)
      case LongType => parentWriter.bigInt(col.name)
      case TimestampType => parentWriter.timeStamp(col.name)
      case FloatType => parentWriter.float4(col.name)
      case DoubleType => parentWriter.float8(col.name)
      case StringType => parentWriter.varChar(col.name)
      case BinaryType => parentWriter.varBinary(col.name)
      case dt: DecimalType => parentWriter.decimal(col.name, dt.scale, dt.precision)
      case struct: StructType =>
        val mapWriter = parentWriter.map(col.name)
        struct.fields.map(getChildWriter(mapWriter, _))
        mapWriter
      case array: ArrayType =>
        val writer = parentWriter.list(col.name)
        writer
      case t: MapType =>
        // TODO Better way for this?
        val writer = parentWriter.map(col.name)
        val key = writer.list("key")
        val value = writer.list("key")
        writer
    }
  }

  // TODO: Consider changing output tp BaseWriter, Option[Seq[BaseWriter]]
  def getChildWriter(parentWriter: ListWriter, col: DataType): BaseWriter = {
    col match {
      case NullType | BooleanType => parentWriter.bit()
      case ByteType => parentWriter.tinyInt()
      case ShortType => parentWriter.smallInt()
      case IntegerType => parentWriter.integer()
      case DateType => parentWriter.date()
      case LongType => parentWriter.bigInt()
      case TimestampType => parentWriter.timeStamp()
      case FloatType => parentWriter.float4()
      case DoubleType => parentWriter.float8()
      case StringType => parentWriter.varChar()
      case BinaryType => parentWriter.varBinary()
      // TODO: Scala and precision?
      case dt: DecimalType => parentWriter.decimal()
      case struct: StructType =>
        val mapWriter = parentWriter.map()
        struct.fields.map(getChildWriter(mapWriter, _))
        mapWriter
      case array: ArrayType =>
        val writer = parentWriter.list()
        getChildWriter(writer, array.elementType)
        writer
      case t: MapType =>
        // TODO Better way for this?
        val writer = parentWriter.map()
        val key = writer.list("key")
        val value = writer.list("key")
        writer
    }

    def writeArrayData(allocator: BaseAllocator, writer: ListWriter, elementType: DataType, arr: ArrayData): Unit = {
      writer.startList()
      val child = getChildWriter(writer, elementType)
      arr.foreach(elementType, (int, obj) => {
        child.setPosition(int)
        if (obj != null) {
          writeValue(allocator, child, elementType, obj)
        }
      })
      writer.endList()
    }

    def writeValue(allocator: BaseAllocator, writer: BaseWriter, dataType: DataType, value: Any): Unit = {
      if (value != null) {
        dataType match {
          case NullType | BooleanType =>
            writer.asInstanceOf[BitWriter].writeBit(if (value.asInstanceOf[Boolean]) 1 else 0)
          case ByteType =>
            writer.asInstanceOf[TinyIntWriter].writeTinyInt(value.asInstanceOf[Byte])
          case ShortType =>
            writer.asInstanceOf[SmallIntWriter].writeSmallInt(value.asInstanceOf[Byte])
          case IntegerType =>
            writer.asInstanceOf[IntWriter].writeInt(value.asInstanceOf[Int])
          case DateType =>
            writer.asInstanceOf[DateWriter].writeDate(value.asInstanceOf[Long])
          case LongType =>
            writer.asInstanceOf[BigIntWriter].writeBigInt(value.asInstanceOf[Long])
          case TimestampType =>
            writer.asInstanceOf[TimeStampWriter].writeTimeStamp(value.asInstanceOf[Long])
          case FloatType =>
            writer.asInstanceOf[Float4Writer].writeFloat4(value.asInstanceOf[Float])
          case DoubleType =>
            writer.asInstanceOf[Float8Writer].writeFloat8(value.asInstanceOf[Double])
          case StringType =>
            val vcw = writer.asInstanceOf[VarCharWriter]
            val a: Array[Byte] = value match {
              case s: UTF8String => s.getBytes
              case s: String => s.getBytes(StandardCharsets.UTF_8)
              case _ => ???
            }
            val b = allocator.buffer(a.length)
            b.setBytes(0, a)
            vcw.writeVarChar(0, a.length, b)
            b.release()
          case BinaryType =>
            val vcw = writer.asInstanceOf[VarBinaryWriter]
            val a = value.asInstanceOf[Array[Byte]]
            val b = allocator.buffer(a.length)
            b.setBytes(0, a)
            vcw.writeVarBinary(0, a.length, b)
            b.release()
          case dt: DecimalType =>
            throw new RuntimeException("DecimalType is unsupported for now.")

          case struct: StructType =>
            val mapWriter = writer.asInstanceOf[MapWriter]
            val row = value.asInstanceOf[InternalRow]
            mapWriter.start()
            struct.fields.zipWithIndex.foreach { case (cf: StructField, ix: Int) =>
              val writer = getChildWriter(mapWriter, cf)
              writeValue(allocator, writer, cf.dataType, row.get(ix, cf.dataType))
            }
            mapWriter.end()

          case array: ArrayType =>
            val listWriter = writer.asInstanceOf[ListWriter]
            val arr = value.asInstanceOf[ArrayData]
            writeArrayData(allocator, listWriter, array.elementType, arr)
          case t: MapType =>
            // TODO Better way for this?
            val listWriter = writer.asInstanceOf[MapWriter]
            val md = value.asInstanceOf[MapData]
            listWriter.start()
            val keys = listWriter.list("key")
            writeArrayData(allocator, keys, t.keyType, md.keyArray())
            val values = listWriter.list("key")
            writeArrayData(allocator, values, t.valueType, md.valueArray())
            listWriter.end()
        }
      }
    }



  def toArrow(df: Dataset[InternalRow]) = {

    df.mapPartitions { it =>
      val allocator = new RootAllocator(Integer.MAX_VALUE)
      val vectorAllocator = allocator.newChildAllocator("records", 0, Integer.MAX_VALUE)

      val parent = new NullableMapVector("parent", vectorAllocator, null)
      val writer = new ComplexWriterImpl("root", parent)
      val rootWriter: MapWriter = writer.rootAsMap()

      // Field types
      df.schema.foreach(getChildWriter(rootWriter, _))

      val length = it.count { ir =>
        writeValue(allocator, rootWriter, df.schema, ir)
        true
      }

      writer.setValueCount(length)

      val vectorUnloader = new VectorUnloader(parent)
      val schema = vectorUnloader.getSchema

      // TODO ByteArrayOutputStream
      val fileOutputStream = new FileOutputStream("asdasd")
      val arrowWriter = new ArrowWriter(fileOutputStream.getChannel, schema)
      arrowWriter.writeRecordBatch(vectorUnloader.getRecordBatch)
      List(1).toIterator
    }
  }

}