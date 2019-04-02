package com.zooz.kafkas3connector.utils

import play.api.libs.json._
import scala.language.implicitConversions

/** Some helper methods to be added to the JsObject type  */
class JsonUtils (baseJson: JsObject) {
  /** Extracts a string from a given Lookup Result and adds it to a given Json object
   * (if the lookup is defined)
   */
  def addFieldFromPath(
      pathLookup: JsLookupResult, newFieldName: String): JsObject = {
    if (pathLookup.isDefined) {
      val extractedField = { 
      pathLookup.get match {
        case _: JsString => pathLookup.as[JsString]
        case _: JsBoolean => pathLookup.as[JsBoolean]
        case _: JsObject => pathLookup.as[JsObject]
        case _: JsNumber => pathLookup.as[JsNumber]
        case _ => pathLookup.as[JsString]
      }}
      baseJson + (newFieldName, extractedField)
    } else {
      baseJson
    }
  }
}

object JsonUtilsImplicits {
  implicit def wrapJsObject(jsObject: JsObject) = new JsonUtils(jsObject)
}
