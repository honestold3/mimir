package org.matruss.mimir.classifier

import org.scalatest.WordSpec
import scala.io.Source

class FastClassificationRuleSpec extends WordSpec
{
  val outputGood = List("travelhome.com", "http://usnews.com", "aolradio.slacker.com")
  val outputBad = List("http://www.recipe.com", "http://worldgames.net")
  val categoriesGood = Source.fromFile("fcr-classifier/src/test/input/categories-good.json").getLines().mkString;
  val categoriesBad = Source.fromFile("fcr-classifier/src/test/input/categories-bad.json").getLines().mkString;
  val rulesGood = Source.fromFile("fcr-classifier/src/test/input/sample-rule-file-good.json").getLines().mkString;
  val rulesBad = Source.fromFile("fcr-classifier/src/test/input/sample-rule-file-bad.json").getLines().mkString;

  "Fast Classification Rules" should {
    "be valid if build from valid source" in {
      val rules = FastClassificationRule.build(rulesGood)

      assert( FastClassificationRule.validateSelf(rules) )
    }
    "be valid if build from valid source and all categories are in a master list" in {
      val rules = FastClassificationRule.build(rulesGood)
      val categories = Category.build(categoriesGood)

      assert( FastClassificationRule.validateAgainst(rules)(categories) )
    }
    "be invalid if build from invalid source" in {
      val rules = FastClassificationRule.build(rulesBad)

      assert( !FastClassificationRule.validateSelf(rules) )
    }
    "be invalid if build from valid source but some categories are not in a master list" in {
      val rules = FastClassificationRule.build(rulesBad)
      val categories = Category.build(categoriesBad)

      assert( !FastClassificationRule.validateAgainst(rules)(categories) )
    }
  }
  "Fast Classification Rules" when {
    "URLs have matching rule" should {
      val rules = FastClassificationRule.build(rulesGood)

      "assign categories to URLs" in {
        outputGood foreach { entry => {
            val cats = rules map { _.assignCategory(entry) } filter { cat =>  cat.isDefined }
            assert(cats.size > 0)
          }
        }
      }
    }
    "rules do not have matching URLs" should {
      val rules = FastClassificationRule.build(rulesGood)

      "do not assign categories to URLs" in {
        outputBad foreach { entry => {
            val cats = rules map { _.assignCategory(entry) } filter { cat =>  cat.isDefined }
            assert(cats.size === 0)
          }
        }
      }
    }
  }
  "Fast Classification Rules" should {
    "be comparable" in {
      import FastClassificationRule._
      val s1 = "{ \"rules\": [ { \"url\": \"cnn.com\", \"cat\": \"News\" } ] }"
      val s2 = "{ \"rules\": [ {\"url\": \"cnn.com/TECH\", \"cat\": \"Technology & Computing\" } ] }"
      val (one, two) = (build(s1), build(s2))
      assert (one.head.compare(two.head) < 0)

      val s3 = "{ \"rules\": [ { \"url\": \"arts.com/2013/\\\\\\\\d+/15/music\", \"cat\": \"News\" } ] }"
      val s4 = "{ \"rules\": [ {\"url\": \"arts.com/2013/\\\\\\\\d+/15/music/boo\", \"cat\": \"Technology & Computing\" } ] }"
      val (three, four) = (build(s3), build(s4))
      assert (three.head.compare(four.head) < 0)
    }
    "generate Assigned Categories with the same level of specificity" in {
      import FastClassificationRule._
      val s1 = "{ \"rules\": [ { \"url\": \"cnn.com\", \"cat\": \"News\" } ] }"
      val s2 = "{ \"rules\": [ {\"url\": \"cnn.com/TECH\", \"cat\": \"Technology & Computing\" } ] }"
      val (one, two) = (build(s1), build(s2))
      val (cat1, cat2)  = (one.head.assignCategory("http://cnn.com"), two.head.assignCategory("http://cnn.com/TECH"));

      assert(one.head.specificity == cat1.head.specificity)
      assert(two.head.specificity == cat2.head.specificity)
    }
  }
}

