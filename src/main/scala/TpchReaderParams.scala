package org.tpch.tablereader

import org.tpch.config.Config
import org.tpch.pushdown.options.TpchPushdownOptions

case class TpchReaderParams(inputDir: String,
                            pushOpt: TpchPushdownOptions,
                            partitions: Int,
                            filePart: Boolean,
                            hostName: String,
                            options: String,
                            config: Config)

object TpchReaderParams {
    def apply(config: Config): TpchReaderParams = {
        new TpchReaderParams(config.inputDir,
                             config.pushdownOptions,
                             config.partitions, config.filePart,
                             config.s3HostName, config.options, config)
    }
}