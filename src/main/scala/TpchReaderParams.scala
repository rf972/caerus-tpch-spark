package org.tpch.tablereader

import org.tpch.filetype._
import org.tpch.pushdown.options.TpchPushdownOptions

case class TpchReaderParams(inputDir: String,
                            pushOpt: TpchPushdownOptions,
                            fileType: FileType,
                            partitions: Int,
                            filePart: Boolean)