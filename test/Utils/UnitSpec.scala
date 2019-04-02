package Utils

import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks

abstract class UnitSpec extends FunSpec with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfterEach with TableDrivenPropertyChecks
