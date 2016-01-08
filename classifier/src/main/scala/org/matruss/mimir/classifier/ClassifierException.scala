package org.matruss.mimir.classifier

trait ClassifierException;

case class ParseException(message:String) extends Exception(message)
case class NoMatchingInputFiles(message:String) extends Exception(message)
case class NoOutputFileSpecified(message:String) extends Exception(message)
case class JobFailed(message:String) extends Exception(message)
case class ValidationFailed(message:String) extends Exception(message)
