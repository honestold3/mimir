package org.matruss.mimir.classifier

import collection.mutable.{Seq,Buffer}

/**
 * Class representing category assigned to URL after rule application
 * @param name    name of the category, i.e. "News"
 * @param score   similarity score - not needed for FCR, but needed since it's the same for bag-of-word classifier
 * @param specificity   @see FastClassificationRule class, similar meaning
 */
class AssignedCategory(val name:String, var score:Double, val specificity:Int)
{
  /**
   * Compare categories based on their specificity
   * @param that  another assigned category
   * @return  :
   *          more than 0 : current category is more specific
   *          equal to 0 :  both categories have the same specificity
   *          less than 0:  another category is more specific
   */
  def compare(that:AssignedCategory):Int = (this.specificity - that.specificity)

  override def toString:String = "[" + name + "," + specificity + "," + score + "]"
}

object AssignedCategory
{
  /**
   * Iterates over sorted list (wrt specificity) of assigned categories, comparing
   * all categories with head value (most specific). If second value after the head is less specific,
   * iteration breaks and only head value is ultimately assigned to URL. If it has the same specificity, category
   * is being assigned as well, etc. Iteration breaks when specificity decreases
   * @param cats  list of would-be assigned categories
   * @return      sequence of assinged categories
   */
  def applyRule(cats:List[AssignedCategory]):Seq[AssignedCategory] =
  {
    val res = Buffer[AssignedCategory](cats.head)
    val iTail = cats.tail.iterator takeWhile { cat => cat.compare( res(0) ) == 0 }
    while (iTail.hasNext) res += iTail.next()
    res
  }
}

case class Category(langid:String, catid:String, level:String, parentid:String, name:String);
case class CategoryList(categories:List[Category]) { val elems = categories; }

object Category extends io.ExternalData
{
  type Value = Category
  type ValueSeq = CategoryList
}

// case classes for rules list parsing
case class RuleList(rules:List[FastClassificationRule])  { val elems = rules; }

/**
 * Case class for rules list parsing, representing matching rules
 * @param url   url pattern, Java-style regex
 * @param cat   category name
 */
case class FastClassificationRule(url:String, cat:String)
{
  private[this] val regex = {
    try { Some(url.r) }
    catch{ case e:Exception => None }
  }

  /**
   * Rule (and assigned category) is considered specific:
   * "cnn.com"  : least specific (only domain)
   * "cnn.com/tech":  more specific
   * "cnn.com/tech/cars": most specific
   */
  val specificity = url.split('/').size

  private[this] val assignedCategory = new AssignedCategory(cat, 1.0, specificity)

  def isValid:Boolean = regex.isDefined && url.size > 0 && cat.size > 0

  /**
   * Compare rules based on their specificity
   * @param that  another rule
   * @return  :
   *          more than 0 : current rule is more specific
   *          equal to 0 :  both rules have the same specificity
   *          less than 0:  another rule is more specific
   */
  def compare(that:FastClassificationRule):Int = (this.specificity - that.specificity)

  def assignCategory(elem:String):Option[AssignedCategory] =
  {
    regex.get.findFirstIn(elem) match {
      case Some(value) => Some(assignedCategory)
      case None => None
    }
  }
}

object FastClassificationRule extends io.ExternalData
{
  type Value = FastClassificationRule
  type ValueSeq = RuleList

  /**
   * Rule is considered valid if its regex can be compiled, and both url/pattern and category are defined
   * If either rule is invalid, whole list is considered invalid
   *
   * @param rules
   * @return
   */
  def validateSelf(rules:List[FastClassificationRule]):Boolean =
  {
    if (rules.size > 0)
      if (rules.forall(_.isValid)) true;   // all rules are valid
      else {
        // some are not, since we are not proceeding anyway, cycle through all, examine each, and print message
        rules filterNot { rule => rule.isValid} foreach { rule =>
          logger.error("ERROR: rule with pattern: " + rule.url + " and category: " + rule.cat + " failed validation")
        }
        false
      }
    else {
      logger.error("ERROR: size of rules file is " + rules.size)
      false
    }
  }

  /**
   * Rule is considered valid only if corresponding categories's name is in master category file
   *
   * @param rules       rules to check
   * @param categories  master categories file
   * @return            either valid or not
   */
  def validateAgainst(rules:List[FastClassificationRule])(categories:List[Category]):Boolean =
  {
    if (rules.size > 0 && categories.size > 0) {
      val areCategoriesNamesValid = rules.forall(r => categories.exists(_.name == r.cat))
      if (!areCategoriesNamesValid) {
        val rNames = rules map( _.cat )
        val cNames = categories map(_.name)
        rNames filterNot { name => cNames.contains(name) } foreach { name =>
          logger.error("category: " + name + " is not in master category file")
        }
      }
      areCategoriesNamesValid
    }
    else {
      logger.error("size of rules file is " + rules.size + "; size of categories file is " + categories.size);
      false
    }
  }

  // currently not used but might be used in a future
  def normalize(categories:Seq[AssignedCategory]):Seq[AssignedCategory] =
  {
    val weight = 1./(categories.size)
    categories map { elem => new AssignedCategory(elem.name, weight, elem.specificity) }
  }
}
