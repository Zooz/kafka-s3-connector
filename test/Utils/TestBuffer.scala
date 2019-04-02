package Utils

import better.files.File.root
import com.zooz.kafkas3connector.buffers.Buffer
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetricsSpec
import play.api.Configuration

class TestBuffer(conf: Configuration)(
  fileName: String,
  newName: String) extends Buffer(conf, fileName) {

  override def copyBuffer(): Unit = {
    val newFile =
      (root / BufferManagerMetricsSpec.rootFilePath / newName)
        .createFileIfNotExists(createParents = true)
    file.copyTo(newFile, overwrite = true)
  }
}
