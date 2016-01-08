package org.matruss.mimir.classifier

import io.{InputSourceResolver, InputSource}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.log4j.Logger

/**
 * This trait defines mapper logic and abstracts away types of output (and, possibly input) values
 * It is necessary, since mapper should output result both as regular writables, and Avro writables,
 * which are going to have different types
 */
trait MapResolver
{
  // define abstract types for generic mapper
  type KeyOut = Writable
  type ValueOut = Writable

  // define abstract types for actual writable
  // default output is for unclassified urls, named output for classified one
  // KeyOut type is the same for regular and named outputs, ValueOut type is different in both cases
  type KeyOutDefault <: Writable
  type ValueOutDefault <: Writable
  type ValueOutNamed <: Writable

  type MapperType = Mapper[LongWritable,Text,KeyOut,ValueOut]

  protected def defaultMapperKey(url:String):KeyOut
  protected def defaultMapperValue:ValueOutDefault
  protected def namedMapperValue(cats:List[AssignedCategory]):ValueOutNamed

  abstract class ContentProcessingMapper extends MapperType
  {
    protected def writeNamed(key:Writable, value:Writable, path:String):Unit

    protected var mOut:Option[MultipleOutputs[KeyOut, ValueOut]] = None
    protected var oContext:Option[MapperType#Context] = None
    protected var oRules:Option[List[FastClassificationRule]] = None
    protected var oClassifiedPath:Option[String] = None
    protected lazy val logger = Logger.getLogger(getClass)

    protected def rules(opt:Option[Any]):Option[List[FastClassificationRule]] =
    {
      var src:Option[InputSource] = None
      if (opt.isDefined){
        val context = opt.get.asInstanceOf[MapperType#Context]

        object LocalStash extends InputSourceResolver
        {
          override val config = context.getConfiguration
          def getSource(cls:Symbol, name:String):InputSource = (sources.get(cls).get)(name)
        }
        // no matter where was the original file, cached file will be on a local file system
        val loc = Symbol("file")
        src = {
          val name = context.getConfiguration.get("rules_link_name","rules.json")
          try { Some(LocalStash.getSource( loc, name ) ) }
          catch { case e:Exception => {
            logger.warn("In mappers rules(): " + e.getMessage)
            logger.warn("In mappers rules(): " + e.getStackTraceString)
            None
          }}
        };
      }
      FastClassificationRule.build( src )
    }

    override def setup(context:MapperType#Context)
    {
      mOut = Some( new MultipleOutputs[KeyOut, ValueOut](context))
      oContext = Some(context)

      oRules = rules(Some(context))
      oClassifiedPath = {
        val path = context.getConfiguration.get("classified_output_path","")
        val part = "part"
        Some(path + "/" + part)
      }
    }

    /**
     * This mapping takes input record from content fetcher, which is, tab-separated "url,content,frequenxy"
     * It splits it and take just a url, then matching it to each FCR rule get list of would-be assigned categories.
     * If there is no match: input record get written to default output unchanged;
     * If there is a match(s):
     * - only one match : corresponding category is assigned
     * - multiple matches:
     * - - if they have different specificity ( @see FastClassificationRule class) ): the category with the highest value is assigned
     * - - if there is more than one category with highest specificity: all such categories are assigned, all with the same score 1.0
     *
     * @param inKey     input key
     * @param inValue   input record of the form "url, content, frequency", tab-separated
     * @param context   mapper context
     */
    override def map(inKey:LongWritable, inValue:Text, context:MapperType#Context)
    {
      // takes full record, splits it, drops content, analyses url
      val url = {
        val buf = new StringBuilder(inValue.toString)
        buf.subSequence(0,buf.indexOf('\t'))
      } toString
      val wouldBeCats = {
        if (oRules.isDefined) oRules.get map { rule => rule.assignCategory(url) } filter { elem => elem.isDefined } map {_.get};
        else {
          logger.warn("No rules file, everything will go into unclassified output")
          List[AssignedCategory]()
        }
      }
      logger.debug("for url " + url + " assigned categories are : " + wouldBeCats.map(_.toString) )
      if ( wouldBeCats.size > 0 ) {
        val outKey = defaultMapperKey(url)
        val sortedCats = wouldBeCats sortWith {_.compare(_) > 0}
        val outValue = namedMapperValue (sortedCats)
        writeNamed(outKey,  outValue, oClassifiedPath.get)
      }
      else {
        context.write(inValue, defaultMapperValue )
      }
    }
  }
}

class TextKeyMapper extends TextKeyMapper.ContentProcessingMapper
{
  protected def writeNamed(key:Writable, value:Writable, path:String)
  {
    mOut.get.write("classified",key,value,path)
  }
}

/**
 * MapperResolver implementation for writing non-Avro output to HDFS
 */
object TextKeyMapper extends MapResolver
{
  type KeyOutDefault = Text
  type ValueOutDefault = NullWritable
  type ValueOutNamed = MapWritable

  protected def defaultMapperKey(url:String):Text = new Text(url)
  protected def defaultMapperValue:ValueOutDefault = NullWritable.get()
  protected def namedMapperValue(cats:List[AssignedCategory]):MapWritable = {
    val res = new MapWritable
    AssignedCategory.applyRule(cats) map { cat => res.put( new Text(cat.name), new DoubleWritable(cat.score) )}
    res
  }
}
