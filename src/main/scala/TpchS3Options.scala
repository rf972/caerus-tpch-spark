package org.tpch.s3options

case class TpchS3Options(enableFilter: Boolean,
                         enableProject: Boolean,
                         enableAggregate: Boolean,
                         explain: Boolean) {

    def isEnabled() : Boolean = {
      enableFilter && enableProject && enableAggregate
    }
}