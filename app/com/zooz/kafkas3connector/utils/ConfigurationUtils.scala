package com.zooz.kafkas3connector.utils

import play.api.Configuration
import play.api.Configuration._
import play.api.ConfigLoader

/** Some helper methods used in parsing Play's configuration
 *  
 *  @param conf a handle to Play's configuration 
 */
class ConfigurationUtils(conf: Configuration) {
  
  /** Validates that a parameter, set as non-mandatory in the application.conf file,
   * was indeed set and returns its value
   */
  def getMandatoryConf[A](param: String)(implicit loader: ConfigLoader[A]): A = {
    val valueOption = conf.getOptional[A](param)(loader)
    if (valueOption.isEmpty) {
      throw new IllegalArgumentException(s"Environment variable for $param was not set")
    } else {
      valueOption.get
    }
  }
}
