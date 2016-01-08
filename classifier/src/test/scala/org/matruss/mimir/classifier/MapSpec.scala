package org.matruss.mimir.classifier

import org.scalatest.{BeforeAndAfter, WordSpec}
import org.apache.hadoop.io._
import org.apache.hadoop.mrunit.mapreduce.MultiOutputMapDriver
import org.apache.hadoop.mrunit.mapreduce.MapDriver
import io.{InputSource, LocalFile}

class MapSpec extends WordSpec with BeforeAndAfter
{
  object MockTextKeyMapper extends MapResolver
  {
    type KeyOutDefault = Text
    type ValueOutDefault = NullWritable
    type ValueOutNamed = MapWritable

    protected def defaultMapperKey(url:String):Text = new Text(url);
    protected def defaultMapperValue:ValueOutDefault = NullWritable.get();
    protected def namedMapperValue(cats:List[AssignedCategory]):MapWritable = {
      val res = new MapWritable;
      AssignedCategory.applyRule(cats) map { cat => res.put( new Text(cat.name), new DoubleWritable(cat.score) )};
      res;
    };
  }

  "FCR Mapper" should {

    class MockTextKeyMapper extends MockTextKeyMapper.ContentProcessingMapper
    {
      override protected def rules(opt:Option[Any]):Option[List[FastClassificationRule]] =
      {
        val src = Some(new LocalFile("fcr-classifier/src/test/input/sample-rule-file-good.json"));
        FastClassificationRule.build( src )
      }
      protected def writeNamed(key:Writable, value:Writable, path:String)
      {
        oContext.get.write(key,value);
      }
    };

    "output classifiable entries in a special output" in {
      val (url,body) = ("http://travelcity.com", "<html><body>Ads go here</body></html>" + "\t" + "10");
      val valueOut = {
        val res = new MapWritable;
        res.put(new Text("Travel"), new DoubleWritable(1.0));
        res;
      }
      val driver = MapDriver.newMapDriver( new MockTextKeyMapper );
      driver.withInput( new LongWritable(1), new Text(url + "\t" + body) );
      driver.withOutput( new Text(url), valueOut );

      driver.runTest();
    }
    "output classifiable entries choosing most specific pattern" in {
      val (url,body) = ("http://usnews.com/election", "<html><body>Tech News</body></html>" + "\t" + "10");
      val valueOut = {
        val res = new MapWritable;
        res.put(new Text("News || Politics"), new DoubleWritable(1.0));
        res;
      }
      val driver = MapDriver.newMapDriver( new MockTextKeyMapper );
      driver.withInput( new LongWritable(1), new Text(url + "\t" + body) );
      driver.withOutput( new Text(url), valueOut );

      driver.runTest();
    }
    "output non-classified entries in a special output" in {
      val (url,body) = ("http://www.kremlin.ru", "<html><body>Russian World</body></html>" + "\t" + "10");
      val driver = new MultiOutputMapDriver( new MockTextKeyMapper );
      driver.withInput( new LongWritable(1), new Text(url + "\t" + body) );
      driver.withOutput( new Text(url + "\t" + body), NullWritable.get() );

      driver.runTest();
    }
  }
}
