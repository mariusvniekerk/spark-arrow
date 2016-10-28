package org.apache.spark.sql.arrow

import java.io.FileOutputStream
import java.lang.Boolean
import java.nio.charset.{Charset, StandardCharsets}

import org.apache.arrow.memory.{BaseAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl
import org.apache.arrow.vector.complex.writer.BaseWriter.{ListWriter, MapWriter}
import org.apache.arrow.vector.complex.writer._
import org.apache.arrow.vector.file.ArrowWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ObjectConsumer, ObjectProducer}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object DataframeToArrow {

  val UTF8 = Charset.forName("UTF-8")

  // TODO: Consider changing output tp BaseWriter, Option[Seq[BaseWriter]]
  def getChildWriter(parentWriter: MapWriter, name: String, dataType: DataType): BaseWriter = {
    dataType match {
      case NullType | BooleanType => parentWriter.bit(name)
      case ByteType => parentWriter.tinyInt(name)
      case ShortType => parentWriter.smallInt(name)
      case IntegerType => parentWriter.integer(name)
      case DateType => parentWriter.date(name)
      case LongType => parentWriter.bigInt(name)
      case TimestampType => parentWriter.timeStamp(name)
      case FloatType => parentWriter.float4(name)
      case DoubleType => parentWriter.float8(name)
      case StringType => parentWriter.varChar(name)
      case BinaryType => parentWriter.varBinary(name)
      case dt: DecimalType => parentWriter.decimal(name, dt.scale, dt.precision)
      case struct: StructType =>
        val mapWriter = parentWriter.map(name)
        struct.fields.map(f => getChildWriter(mapWriter, f.name, f.dataType))
        mapWriter
      case array: ArrayType =>
        val writer = parentWriter.list(name)
        writer
      case t: MapType =>
        // TODO Better way for this?
        val writer = parentWriter.map(name)
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
        struct.fields.map(f => getChildWriter(mapWriter, f.name, f.dataType))
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
              val writer = getChildWriter(mapWriter, cf.name, cf.dataType)
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

  def toArrow(df: RDD[InternalRow], attributes: Seq[Attribute]): RDD[InternalRow] = {

    df.mapPartitions { it =>
      val allocator = new RootAllocator(Integer.MAX_VALUE)
      val vectorAllocator = allocator.newChildAllocator("records", 0, Integer.MAX_VALUE)

      val parent = new NullableMapVector("parent", vectorAllocator, null)
      val writer = new ComplexWriterImpl("root", parent)
      val rootWriter: MapWriter = writer.rootAsMap()

      val fieldWriters = attributes.map{
        attr =>
           val w = getChildWriter(rootWriter, attr.name, attr.dataType)
           w
      }

      val length = it.count { ir =>
        fieldWriters.zip(attributes) foreach {
          case (w, a) => writeValue(allocator, w, a.dataType, a.eval(ir))
        }
        true
      }

      writer.setValueCount(length)

      val vectorUnloader = new VectorUnloader(parent)
      val schema = vectorUnloader.getSchema

      // TODO ByteArrayOutputStream
      val fileOutputStream = new FileOutputStream("asdasd")
      val arrowWriter = new ArrowWriter(fileOutputStream.getChannel, schema)
      arrowWriter.writeRecordBatch(vectorUnloader.getRecordBatch)

      Iterator(InternalRow(Array.empty[Byte]))
    }
  }

}


case class ArrowExec(outputObjAttr: Attribute, child: SparkPlan) extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {

    DataframeToArrow.toArrow(
      child.execute(),
      child.output
    )
  }

  override def output: Seq[Attribute] = outputObjAttr :: Nil

}


case class ArrowLogical(
  outputObjAttr: Attribute,
  child: LogicalPlan) extends ObjectConsumer with ObjectProducer  {

}

object ArrowLogical {

  def apply(child: LogicalPlan): LogicalPlan = {

    val attr = AttributeReference("arrow_blob", BinaryType, nullable = false)()

    ArrowLogical(
      attr,
      child
    )

  }

}


object ArrowOperators extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ArrowLogical(o, child) =>
      ArrowExec(o, planLater(child)) :: Nil
  }
}
