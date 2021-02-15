package org.tpch.s3options

case class TpchS3Options(enableFilter: Boolean,
                         enableProject: Boolean,
                         enableAggregate: Boolean,
                         explain: Boolean) {

    def isPushdownEnabled() : Boolean = {
      enableFilter && enableProject && enableAggregate
    }
}